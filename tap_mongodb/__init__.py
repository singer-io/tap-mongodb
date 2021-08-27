#!/usr/bin/env python3
import copy
import json
import logging
import ssl
import sys
import pymongo
from pymongo.collection import Collection
from bson import errors

import singer
from singer import metadata, metrics, utils

import tap_mongodb.sync_strategies.common as common
import tap_mongodb.sync_strategies.full_table as full_table
import tap_mongodb.sync_strategies.oplog as oplog
import tap_mongodb.sync_strategies.incremental as incremental

from pymongo_schema import extract
import time

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password',
    'database'
]

IGNORE_DBS = ['system', 'local', 'config']
ROLES_WITHOUT_FIND_PRIVILEGES = {
    'dbAdmin',
    'userAdmin',
    'clusterAdmin',
    'clusterManager',
    'clusterMonitor',
    'hostManager',
    'restore'
}
ROLES_WITH_FIND_PRIVILEGES = {
    'read',
    'readWrite',
    'readAnyDatabase',
    'readWriteAnyDatabase',
    'dbOwner',
    'backup',
    'root'
}
ROLES_WITH_ALL_DB_FIND_PRIVILEGES = {
    'readAnyDatabase',
    'readWriteAnyDatabase',
    'root'
}
STEP_LIMIT = 1000000


def get_roles(client, config):
    # usersInfo Command returns object in shape:
    # {
    #     <some_other_keys>
    #     'users': [
    #         {
    #             '_id': <auth_db>.<user>,
    #             'db': <auth_db>,
    #             'mechanisms': ['SCRAM-SHA-1', 'SCRAM-SHA-256'],
    #             'roles': [{'db': 'admin', 'role': 'readWriteAnyDatabase'},
    #                       {'db': 'local', 'role': 'read'}],
    #             'user': <user>,
    #             'userId': <userId>
    #         }
    #     ]
    # }
    user_info = client[config['database']].command({'usersInfo': config['user']})

    users = [u for u in user_info.get('users') if u.get('user') == config['user']]
    if len(users) != 1:
        LOGGER.warning('Could not find any users for %s', config['user'])
        return []

    roles = []
    for role in users[0].get('roles', []):
        if role.get('role') is None:
            continue

        role_name = role['role']
        # roles without find privileges
        if role_name in ROLES_WITHOUT_FIND_PRIVILEGES:
            continue

        # roles with find privileges
        if role_name in ROLES_WITH_FIND_PRIVILEGES:
            if role.get('db'):
                roles.append(role)

        # for custom roles, get the "sub-roles"
        else:
            role_info_list = client[config['database']].command(
                {'rolesInfo': {'role': role_name, 'db': config['database']}})
            role_info = [r for r in role_info_list.get('roles', []) if r['role'] == role_name]
            if len(role_info) != 1:
                continue
            for sub_role in role_info[0].get('roles', []):
                if sub_role.get('role') in ROLES_WITH_FIND_PRIVILEGES:
                    if sub_role.get('db'):
                        roles.append(sub_role)
    return roles


def get_databases(client, config):
    roles = get_roles(client, config)
    LOGGER.info('Roles: %s', roles)

    can_read_all = len([r for r in roles if r['role'] in ROLES_WITH_ALL_DB_FIND_PRIVILEGES]) > 0

    if can_read_all:
        db_names = [d for d in client.list_database_names() if d not in IGNORE_DBS]
    else:
        db_names = [r['db'] for r in roles if r['db'] not in IGNORE_DBS]
    LOGGER.info('Datbases: %s', db_names)
    return db_names


def build_schema_for_type(type):
    # If it's the _id
    if type == 'oid':
        return {
            "inclusion": "automatic",
            "type": "string"
        }

    if type == 'date':
        return {
            "inclusion": "available",
            "type": ["null", "string"],
            "format": "date-time"
        }

    if type == 'ARRAY':
        return {
            "inclusion": "available",
            "type": ["null", "array"],
        }

    if type == 'OBJECT':
        return {
            "inclusion": "available",
            "type": ["null", "object"],
        }

    if type == 'float':
        return {
            "inclusion": "available",
            "type": "number",
        }

    if type == "general_scalar":
        return {
            "inclusion": "available",
            "type": ["null", "mixed"],

        }

    return {
        "inclusion": "available",
        "type": type
    }


def build_schema_for_level(properties):
    schema_properties = {}

    for propertyName, propertyInfo in properties.items():
        property_type = propertyInfo['type']

        # Add the field to the schema
        property_schema = build_schema_for_type(property_type)

        # If we have an object we need to build the schema inside
        if property_type == 'OBJECT':
            property_schema['properties'] = build_schema_for_level(propertyInfo['object'])

        schema_properties[propertyName] = property_schema

    return schema_properties


def _fault_tolerant_extract_collection_schema(collection: Collection, sample_size: int = None):
    """
    @see extract.extract_collection_schema - but catches InvalidBSON errors
    """
    logger = logging.getLogger()
    collection_schema = {
        'count': 0,
        "object": extract.init_empty_object_schema()
    }

    # count is deprecated  DEPRECATED -> could use count_documents({}), but is very slow
    document_count = collection.estimated_document_count()
    collection_schema['count'] = document_count
    # if sample_size:
    #     documents = collection.aggregate([{'$sample': {'size': sample_size}}], allowDiskUse=True)
    # else:
    #     documents = collection.find({})

    # TODO: there are 2 ways to solve large documents data reads per collection
    # 1. with python multi-threads - run in parallel
    # 2. with slice indexes containing a limit and skip
    # The second is implemented. TO check the performance!

    # get a slice of documents
    steps = int(round(document_count / STEP_LIMIT)) + 1
    logger.info('Total number of steps %s', steps)
    start_time = time.time()
    for step in range(steps):
        start = step * STEP_LIMIT
        stop = (step + 1) * STEP_LIMIT
        if stop > document_count:
            stop = document_count
        documents = collection.find({})[start:stop]
        scan_count = stop
        logger.info('Step %s of %s for collection %s --- %s seconds ---', step, steps,
                    collection.name, time.time() - start_time)

        i = 0
        while True:
            try:
                # Iterate over documents per step
                document = next(documents)
            except StopIteration:
                break
            except errors.InvalidBSON as err:
                logger.warning("ignore invalid record: {}".format(str(err)))
                continue

            extract.add_document_to_object_schema(document, collection_schema['object'])
            i += 1
            if i == stop:
                logger.info('Scanned %s documents out of %s (%.2f %%) in step %s', i, scan_count,
                            (100. * i) / scan_count, step)

    logger.info('FInished scanning documents of collection %s', collection.name)
    extract.post_process_schema(collection_schema)
    collection_schema = extract.recursive_default_to_regular_dict(collection_schema)
    return collection_schema


def produce_collection_schema(collection: Collection, client):
    collection_name = collection.name
    collection_db_name = collection.database.name

    is_view = collection.options().get('viewOn') is not None

    # Analyze and build schema recursively
    #schema = extract_pymongo_client_schema(client, collection_names=collection_name)
    try:
        schema = _fault_tolerant_extract_collection_schema(collection, sample_size=None)
        """
        TODO: 
        without sample_size it always downloads all data to extract the schema,
        but with it might not get types for all fields and fail writing
        
        maybe it should load the schema with a sample_size, but in the actual import build the
        schema message out of all data?
        """
    except errors.InvalidBSON as e:
        logging.warning("ignored db {}.{} due to BSON error: {}".format(
            collection_db_name,
            collection_name,
            str(e)
        ))
        return None
    extracted_properties = schema['object']
    schema_properties = build_schema_for_level(extracted_properties)

    propertiesBreadcrumb = []

    for k in schema_properties:
        propertiesBreadcrumb.append({
            "breadcrumb": ["properties", k],
            "metadata": {"selected-by-default": True}
        })

    mdata = {}
    mdata = metadata.write(mdata, (), 'table-key-properties', ['_id'])
    mdata = metadata.write(mdata, (), 'database-name', collection_db_name)
    mdata = metadata.write(mdata, (), 'row-count', collection.estimated_document_count())
    mdata = metadata.write(mdata, (), 'is-view', is_view)

    # write valid-replication-key metadata by finding fields that have indexes on them.
    # cannot get indexes for views -- NB: This means no key-based incremental for views?
    if not is_view:
        valid_replication_keys = []
        coll_indexes = collection.index_information()
        # index_information() returns a map of index_name -> index_information
        for _, index_info in coll_indexes.items():
            # we don't support compound indexes
            if len(index_info.get('key')) == 1:
                index_field_info = index_info.get('key')[0]
                # index_field_info is a tuple of (field_name, sort_direction)
                if index_field_info:
                    valid_replication_keys.append(index_field_info[0])

        if valid_replication_keys:
            mdata = metadata.write(mdata, (), 'valid-replication-keys', valid_replication_keys)

    return {
        'table_name': collection_name,
        'stream': collection_name,
        'metadata': metadata.to_list(mdata) + propertiesBreadcrumb,
        'tap_stream_id': "{}-{}".format(collection_db_name, collection_name),
        'schema': {
            'type': 'object',
            'properties': schema_properties
        }
    }


def do_discover(client, config):
    streams = []

    db_name = config.get("database")
    filter_collections = config.get("filter_collections", [])
    if db_name == "admin":
        databases = get_databases(client, config)
    else:
        databases = [db_name]

    for db_name in databases:
        # pylint: disable=invalid-name
        db = client[db_name]

        collection_names = db.list_collection_names()
        for collection_name in collection_names:
            if collection_name.startswith("system.") or (
                    filter_collections and collection_name not in filter_collections):
                continue

            collection = db[collection_name]
            is_view = collection.options().get('viewOn') is not None
            # TODO: Add support for views
            if is_view:
                continue

            LOGGER.info("Getting collection info for db: %s, collection: %s",
                        db_name, collection_name)
            stream = produce_collection_schema(collection, client)
            if stream is not None:
                streams.append(stream)
    json.dump({'streams': streams}, sys.stdout, indent=2)


def is_stream_selected(stream):
    mdata = metadata.to_map(stream['metadata'])
    is_selected = metadata.get(mdata, (), 'selected')

    # pylint: disable=singleton-comparison
    return is_selected == True


def get_streams_to_sync(streams, state):
    # get selected streams
    selected_streams = [s for s in streams if is_stream_selected(s)]
    # prioritize streams that have not been processed
    streams_with_state = []
    streams_without_state = []
    for stream in selected_streams:
        if state.get('bookmarks', {}).get(stream['tap_stream_id']):
            streams_with_state.append(stream)
        else:
            streams_without_state.append(stream)

    ordered_streams = streams_without_state + streams_with_state

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)
    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s['tap_stream_id'] == currently_syncing,
            ordered_streams))
        non_currently_syncing_streams = list(filter(lambda s: s['tap_stream_id']
                                                              != currently_syncing,
                                                    ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        streams_to_sync = ordered_streams

    return streams_to_sync


def write_schema_message(stream):
    singer.write_message(singer.SchemaMessage(
        stream=common.calculate_destination_stream_name(stream),
        schema=stream['schema'],
        key_properties=['_id']))


def load_stream_projection(stream):
    md_map = metadata.to_map(stream['metadata'])
    stream_projection = metadata.get(md_map, (), 'tap-mongodb.projection')
    if stream_projection == '' or stream_projection == '""' or not stream_projection:
        return None

    try:
        stream_projection = json.loads(stream_projection)
    except:
        err_msg = "The projection: {} for stream {} is not valid json"
        raise common.InvalidProjectionException(err_msg.format(stream_projection,
                                                               stream['tap_stream_id']))

    if stream_projection and stream_projection.get('_id') == 0:
        raise common.InvalidProjectionException(
            "Projection blacklists key property id for collection {}" \
                .format(stream['tap_stream_id']))

    return stream_projection


def clear_state_on_replication_change(stream, state):
    md_map = metadata.to_map(stream['metadata'])
    tap_stream_id = stream['tap_stream_id']

    # replication method changed
    current_replication_method = metadata.get(md_map, (), 'replication-method')
    last_replication_method = singer.get_bookmark(state, tap_stream_id, 'last_replication_method')
    if last_replication_method is not None and (current_replication_method != last_replication_method):
        log_msg = 'Replication method changed from %s to %s, will re-replicate entire collection %s'
        LOGGER.info(log_msg, last_replication_method, current_replication_method, tap_stream_id)
        state = singer.reset_stream(state, tap_stream_id)

    # replication key changed
    if current_replication_method == 'INCREMENTAL':
        last_replication_key = singer.get_bookmark(state, tap_stream_id, 'replication_key_name')
        current_replication_key = metadata.get(md_map, (), 'replication-key')
        if last_replication_key is not None and (current_replication_key != last_replication_key):
            log_msg = 'Replication Key changed from %s to %s, will re-replicate entire collection %s'
            LOGGER.info(log_msg, last_replication_key, current_replication_key, tap_stream_id)
            state = singer.reset_stream(state, tap_stream_id)
        state = singer.write_bookmark(state, tap_stream_id, 'replication_key_name', current_replication_key)

    state = singer.write_bookmark(state, tap_stream_id, 'last_replication_method', current_replication_method)

    return state


def sync_stream(client, stream, state):
    tap_stream_id = stream['tap_stream_id']

    common.COUNTS[tap_stream_id] = 0
    common.TIMES[tap_stream_id] = 0
    common.SCHEMA_COUNT[tap_stream_id] = 0
    common.SCHEMA_TIMES[tap_stream_id] = 0

    md_map = metadata.to_map(stream['metadata'])
    replication_method = metadata.get(md_map, (), 'replication-method')
    database_name = metadata.get(md_map, (), 'database-name')

    stream_projection = load_stream_projection(stream)

    # Emit a state message to indicate that we've started this stream
    state = clear_state_on_replication_change(stream, state)
    state = singer.set_currently_syncing(state, stream['tap_stream_id'])
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    write_schema_message(stream)
    common.SCHEMA_COUNT[tap_stream_id] += 1

    with metrics.job_timer('sync_table') as timer:
        timer.tags['database'] = database_name
        timer.tags['table'] = stream['table_name']

        if replication_method == 'LOG_BASED':
            if oplog.oplog_has_aged_out(client, state, tap_stream_id):
                # remove all state for stream
                # then it will do a full sync and start oplog again.
                LOGGER.info("Clearing state because Oplog has aged out")
                state.get('bookmarks', {}).pop(tap_stream_id)

            # make sure initial full table sync has been completed
            if not singer.get_bookmark(state, tap_stream_id, 'initial_full_table_complete'):
                msg = 'Must complete full table sync before starting oplog replication for %s'
                LOGGER.info(msg, tap_stream_id)

                # only mark current ts in oplog on first sync so tap has a
                # starting point after the full table sync
                if singer.get_bookmark(state, tap_stream_id, 'version') is None:
                    collection_oplog_ts = oplog.get_latest_ts(client)
                    oplog.update_bookmarks(state, tap_stream_id, collection_oplog_ts)

                full_table.sync_collection(client, stream, state, stream_projection)

            oplog.sync_collection(client, stream, state, stream_projection)

        elif replication_method == 'FULL_TABLE':
            full_table.sync_collection(client, stream, state, stream_projection)

        elif replication_method == 'INCREMENTAL':
            incremental.sync_collection(client, stream, state, stream_projection)
        else:
            raise Exception(
                "only FULL_TABLE, LOG_BASED, and INCREMENTAL replication \
                methods are supported (you passed {})".format(replication_method))

    state = singer.set_currently_syncing(state, None)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync(client, catalog, state):
    all_streams = catalog['streams']
    streams_to_sync = get_streams_to_sync(all_streams, state)

    for stream in streams_to_sync:
        sync_stream(client, stream, state)

    LOGGER.info(common.get_sync_summary(catalog))


def main_impl():
    # Check if we use DNS string or not
    args = utils.parse_args([])
    config = args.config

    if 'connection_uri' in config.keys():
        # Connect using the DNS string
        parsedUri = pymongo.uri_parser.parse_uri(config['connection_uri'])
        config['username'] = parsedUri['username']
        config['password'] = parsedUri['password']
        config['authSource'] = parsedUri['options']['authsource']
        config['user'] = parsedUri['username']
        config['database'] = parsedUri.get('database')

        if config.get("database") is None:
            config["database"] = "admin"

        client = pymongo.MongoClient(config['connection_uri'])
        LOGGER.info('Connected to MongoDB host: %s, version: %s',
                    config['connection_uri'].replace(config.get("password"), "********"),
                    client.server_info().get('version', 'unknown'))
    else:
        # Connect using the connection parameters
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)
        config = args.config

        # Default SSL verify mode to true, give option to disable
        verify_mode = config.get('verify_mode', 'true') == 'true'
        use_ssl = config.get('ssl') == 'true'

        connection_params = {"host": config['host'],
                             "port": int(config['port']),
                             "username": config.get('user', None),
                             "password": config.get('password', None),
                             "authSource": config['database'],
                             "ssl": use_ssl,
                             "replicaset": config.get('replica_set', None),
                             "readPreference": 'secondaryPreferred'}

        # NB: "ssl_cert_reqs" must ONLY be supplied if `SSL` is true.
        if not verify_mode and use_ssl:
            connection_params["ssl_cert_reqs"] = ssl.CERT_NONE
        client = pymongo.MongoClient(**connection_params)

        LOGGER.info('Connected to MongoDB host: %s, version: %s',
                    config['host'],
                    client.server_info().get('version', 'unknown'))

    common.INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME = \
        (config.get('include_schemas_in_destination_stream_name') == 'true')

    if args.discover:
        do_discover(client, config)
    elif args.catalog:
        state = args.state or {}
        do_sync(client, args.catalog.to_dict(), state)


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc


if __name__ == "__main__":
    main()
