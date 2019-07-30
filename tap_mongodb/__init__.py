#!/usr/bin/env python3
import copy
import json
import sys
from pymongo import MongoClient


import singer
from singer import metadata, metrics, utils
import tap_mongodb.sync_strategies.common as common
import tap_mongodb.sync_strategies.full_table as full_table


LOGGER = singer.get_logger()


REQUIRED_CONFIG_KEYS = [
    'host',
    'port'
]


IGNORE_DBS = ['admin', 'system', 'local', 'config']



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


def do_discover(client):
    streams = []

    database_names = client.list_database_names()
    for db_name in [d for d in database_names
                    if d not in IGNORE_DBS]:
        db = client[db_name]

        collection_names = db.list_collection_names()
        for collection_name in [c for c in collection_names
                                if not c.startswith("system.")]:
            LOGGER.info("Getting collection info for db: " + db_name + ", collection: " + collection_name)
            collection = db[collection_name]
            streams.append(produce_collection_schema(collection))

    json.dump({'streams' : streams}, sys.stdout, indent=2)



def is_stream_selected(stream):
    mdata = metadata.to_map(stream['metadata'])
    stream_metadata = mdata.get(())

    is_selected = stream_metadata.get('selected')

    return is_selected == True


def get_non_oplog_streams(client, streams, state):
    selected_streams = list(filter(lambda s: is_stream_selected(s), streams))

    streams_with_state = []
    streams_without_state = []

    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream['metadata'])
        replication_method = stream_metadata.get((), {}).get('replication-method')
        stream_state = state.get('bookmarks', {}).get(stream['tap_stream_id'])

        if not stream_state:
            if replication_method == 'LOG_BASED':
                LOGGER.info("LOG_BASED stream %s requires full historical sync", stream['tap_stream_id'])

            streams_without_state.append(stream)
        elif stream_state and replication_method == 'LOG_BASED' and oplog_stream_requires_historical(stream, state):
            LOGGER.info("LOG_BASED stream %s will resume its historical sync", stream['tap_stream_id'])
            streams_with_state.append(stream)
        elif stream_state and replication_method != 'LOG_BASED':
            streams_with_state.append(stream)

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)

    # prioritize streams that have not been processed
    ordered_streams = streams_without_state + streams_with_state

    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s['tap_stream_id'] == currently_syncing and is_valid_currently_syncing_stream(s, state),
            streams_with_state))

        non_currently_syncing_streams = list(filter(lambda s: s['tap_stream_id'] != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        # prioritize streams that have not been processed
        streams_to_sync = ordered_streams

    return streams_to_sync

def is_stream_selected(stream):
    mdata = metadata.to_map(stream['metadata'])
    stream_metadata = mdata.get(())

    is_selected = stream_metadata.get('selected')

    return is_selected == True


def oplog_stream_requires_historical(stream, state):
    oplog_ts_time = singer.get_bookmark(state,
                                        stream['tap_stream_id'],
                                        'oplog_ts_time')

    oplog_ts_inc = singer.get_bookmark(state,
                                        stream['tap_stream_id'],
                                        'oplog_ts_inc')

    max_id_value = singer.get_bookmark(state,
                                       stream['tap_stream_id'],
                                       'max_id_value')

    last_id_fetched = singer.get_bookmark(state,
                                          stream['tap_stream_id'],
                                          'last_id_fetched')

    if (oplog_ts_time and oplog_ts_inc) and (not max_id_value and not last_id_fetched):
        return False

    return True

def is_valid_currently_syncing_stream(selected_stream, state):
    stream_metadata = metadata.to_map(selected_stream['metadata'])
    replication_method = stream_metadata.get((), {}).get('replication-method')

    if replication_method != 'LOG_BASED':
        return True

    if replication_method == 'LOG_BASED' and oplog_stream_requires_historical(selected_stream, state):
        return True

    return False



def get_non_oplog_streams(client, streams, state):
    selected_streams = list(filter(lambda s: is_stream_selected(s), streams))

    streams_with_state = []
    streams_without_state = []

    for stream in selected_streams:
        stream_metadata = metadata.to_map(stream['metadata'])
        replication_method = stream_metadata.get((), {}).get('replication-method')
        stream_state = state.get('bookmarks', {}).get(stream['tap_stream_id'])

        if not stream_state:
            if replication_method == 'LOG_BASED':
                LOGGER.info("LOG_BASED stream %s requires full historical sync", stream['tap_stream_id'])

            streams_without_state.append(stream)
        elif stream_state and replication_method == 'LOG_BASED' and oplog_stream_requires_historical(stream, state):
            LOGGER.info("LOG_BASED stream %s will resume its historical sync", stream['tap_stream_id'])
            streams_with_state.append(stream)
        elif stream_state and replication_method != 'LOG_BASED':
            streams_with_state.append(stream)

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)

    # prioritize streams that have not been processed
    ordered_streams = streams_without_state + streams_with_state

    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s['tap_stream_id'] == currently_syncing and is_valid_currently_syncing_stream(s, state),
            streams_with_state))

        non_currently_syncing_streams = list(filter(lambda s: s['tap_stream_id'] != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        # prioritize streams that have not been processed
        streams_to_sync = ordered_streams

    return streams_to_sync


def write_schema_message(stream):
    singer.write_message(singer.SchemaMessage(
        stream=stream['stream'],
        schema=stream['schema'],
        key_properties=['_id']))



def sync_non_oplog_streams(client, streams, state):
    for stream in streams:
        md_map = metadata.to_map(stream['metadata'])
        stream_metadata = md_map.get(())
        stream_projection = stream_metadata.get('projection')
        # import ipdb
        # ipdb.set_trace()

        if not stream_projection:
            LOGGER.warning('There is no projection found for stream %s, all fields will be retrieved.', stream['tap_stream_id'])

        state = singer.set_currently_syncing(state, stream['tap_stream_id'])

        # Emit a state message to indicate that we've started this stream
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        replication_method = stream_metadata.get('replication-method')

        database_name = stream_metadata.get('database-name')

        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = database_name
            timer.tags['table'] = stream['table_name']
            if replication_method == 'LOG_BASED':
               # do_sync_historical_oplog(client, stream, state, columns)
                continue
            elif replication_method == 'FULL_TABLE':
                write_schema_message(stream)
                stream_version = common.get_stream_version(stream['tap_stream_id'], state)
                full_table.sync_table(client, stream, state, stream_version, stream_projection)

                state = singer.write_bookmark(state,
                                              stream['tap_stream_id'],
                                              'initial_full_table_complete',
                                              True)
            else:
                raise Exception("only LOG_BASED and FULL TABLE replication methods are supported (you passed {})".format(replication_method))

    state = singer.set_currently_syncing(state, None)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    

def do_sync(client, catalog, state):
    all_streams = catalog['streams']
    non_oplog_streams = get_non_oplog_streams(client, all_streams, state)

    sync_non_oplog_streams(client, non_oplog_streams, state)

    
def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    client =  MongoClient(host=config['host'],
                          port=int(config['port']),
                          username=config.get('user', None),
                          password=config.get('password', None),
                          authSource=config['database'])


    if args.discover:
         do_discover(client)
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
