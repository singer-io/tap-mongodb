#!/usr/bin/env python3
import copy
import json
import sys
import time
from pymongo import MongoClient
from bson import timestamp

import singer
from singer import metadata, metrics, utils

import tap_mongodb.sync_strategies.common as common
import tap_mongodb.sync_strategies.full_table as full_table
import tap_mongodb.sync_strategies.oplog as oplog
import tap_mongodb.sync_strategies.incremental as incremental


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password',
    'database'
]

IGNORE_DBS = ['admin', 'system', 'local', 'config']

def get_databases(client, config):

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

    can_read_all = False
    db_names = []

    users = [u for u in user_info.get('users') if u.get('user')==config['user']]
    if len(users)!=1:
        LOGGER.warning('Could not find any users for {}'.format(config['user']))
        return []

    for role_info in users[0].get('roles', []):
        if role_info.get('role') in ['read', 'readWrite']:
            if role_info.get('db'):
                db_names.append(role_info['db'])
        elif role_info.get('role') in ['readAnyDatabase', 'readWriteAnyDatabase']:
            # If user has either of these roles they can query all databases
            can_read_all = True
            break

    if can_read_all:
        db_names =  client.list_database_names()

    return [d for d in db_names if d not in IGNORE_DBS]



def produce_collection_schema(collection):
    collection_name = collection.name
    collection_db_name = collection.database.name

    is_view = collection.options().get('viewOn') is not None

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
        for index_name, index_info in coll_indexes.items():
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
        'metadata': metadata.to_list(mdata),
        'tap_stream_id': "{}-{}".format(collection_db_name, collection_name),
        'schema': {
            'type': 'object'
        }
    }


def do_discover(client, config):
    streams = []

    for db_name in get_databases(client, config):
        db = client[db_name]

        collection_names = db.list_collection_names()
        for collection_name in [c for c in collection_names
                                if not c.startswith("system.")]:

            collection = db[collection_name]
            is_view = collection.options().get('viewOn') is not None
            # TODO: Add support for views
            if is_view:
                continue

            LOGGER.info("Getting collection info for db: " + db_name + ", collection: " + collection_name)
            streams.append(produce_collection_schema(collection))

    json.dump({'streams' : streams}, sys.stdout, indent=2)


def is_stream_selected(stream):
    mdata = metadata.to_map(stream['metadata'])

    stream_metadata = mdata.get(())

    is_selected = stream_metadata.get('selected')

    return is_selected == True


def get_streams_to_sync(client, streams, state):

    # get selected streams
    selected_streams = list(filter(lambda s: is_stream_selected(s), streams))
    # prioritize streams that have not been processed
    streams_with_state = []
    streams_without_state = []
    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream['metadata'])
        stream_state = state.get('bookmarks', {}).get(stream['tap_stream_id'])
        streams_with_state.append(stream) if stream_state else streams_without_state.append(stream)

    ordered_streams = streams_without_state + streams_with_state

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)
    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s['tap_stream_id'] == currently_syncing,
            ordered_streams))
        non_currently_syncing_streams = list(filter(lambda s: s['tap_stream_id'] != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        streams_to_sync = ordered_streams

    return streams_to_sync


def write_schema_message(stream):
    singer.write_message(singer.SchemaMessage(
        stream=common.calculate_destination_stream_name(stream),
        schema=stream['schema'],
        key_properties=['_id']))


def sync_stream(client, stream, state):
    tap_stream_id = stream['tap_stream_id']

    common.COUNTS[tap_stream_id] = 0
    common.TIMES[tap_stream_id] = 0

    md_map = metadata.to_map(stream['metadata'])
    stream_metadata = md_map.get(())

    replication_method = stream_metadata.get('replication-method')
    database_name = stream_metadata.get('database-name')

    stream_projection = stream_metadata.get('tap-mongodb.projection')

    stream_state = state.get('bookmarks', {}).get(stream['tap_stream_id'],{})

    if stream_projection:
        try:
            stream_projection = json.loads(stream_projection)
        except:
            err_msg = "The projection: {} for stream {} is not valid json"
            raise common.InvalidProjectionException(err_msg.format(stream_projection,
                                                                   tap_stream_id))
    else:
        LOGGER.warning('There is no projection found for stream %s, all fields will be retrieved.', stream['tap_stream_id'])

    if stream_projection and stream_projection.get('_id') == 0:
        raise common.InvalidProjectionException("Projection blacklists key property id")

    # Emit a state message to indicate that we've started this stream
    state = singer.set_currently_syncing(state, stream['tap_stream_id'])
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    write_schema_message(stream)

    with metrics.job_timer('sync_table') as timer:
        timer.tags['database'] = database_name
        timer.tags['table'] = stream['table_name']

        if replication_method == 'LOG_BASED':

            # make sure initial full table sync has been completed
            if not stream_state.get('initial_full_table_complete'):
                LOGGER.info('Must complete full table sync before starting oplog replication for {}'.format(tap_stream_id))

                # mark current ts in oplog so tap has a starting point
                # after the full table sync
                collection_oplog_ts = oplog.get_latest_collection_ts(client, stream)
                oplog.update_bookmarks(state, tap_stream_id, collection_oplog_ts)

                full_table.sync_collection(client, stream, state, stream_projection)

            #TODO: Check if oplog has aged out here
            oplog.sync_collection(client, stream, state, stream_projection)

        elif replication_method == 'FULL_TABLE':
            full_table.sync_collection(client, stream, state,  stream_projection)

        elif replication_method == 'INCREMENTAL':
            incremental.sync_collection(client, stream, state, stream_projection)
        else:
            raise Exception("only FULL_TABLE, LOG_BASED, and INCREMENTAL replication methods are supported (you passed {})".format(replication_method))

    state = singer.set_currently_syncing(state, None)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync(client, catalog, state):
    all_streams = catalog['streams']
    streams_to_sync = get_streams_to_sync(client, all_streams, state)

    for stream in streams_to_sync:
        sync_stream(client, stream, state)

    LOGGER.info(common.get_sync_summary(catalog))


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    client =  MongoClient(host=config['host'],
                          port=int(config['port']),
                          username=config.get('user', None),
                          password=config.get('password', None),
                          authSource=config['database'],
                          ssl=(config.get('ssl')=='true'),
                          replicaset=config.get('replica_set', None))


    common.include_schemas_in_destination_stream_name = (config.get('include_schemas_in_destination_stream_name') == 'true')

    if args.discover:
         do_discover(client, config)
    elif args.catalog:
        state = args.state or {}
        do_sync(client, args.catalog.to_dict(), state)
    else:
        LOGGER.info("Only discovery mode supported right now")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
