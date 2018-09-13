from bson import objectid
import copy
import json
from pymongo import MongoClient

import singer
from singer import metadata, metrics, utils

import sys
import time

LOGGER = singer.get_logger()


REQUIRED_CONFIG_KEYS = [
    'host',
    'port',
    'user',
    'password'
]


IGNORE_DBS = ['admin', 'system', 'local']
IGNORE_COLLECTIONS = ['system.indexes']


def produce_collection_schema(collection):
    collection_name = collection.name
    collection_db_name = collection.database.name

    mdata = {}
    mdata = metadata.write(mdata, (), 'database-name', collection_db_name)
    mdata = metadata.write(mdata, (), 'row-count', collection.count())

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
    database_names = client.list_database_names()

    streams = []

    for db_name in [d for d in database_names
                    if d not in IGNORE_DBS]:
        db = client[db_name]
        collection_names = db.list_collection_names()

        for collection_name in [c for c in collection_names
                                if c not in IGNORE_COLLECTIONS]:
            collection = db[collection_name]

            streams.append(produce_collection_schema(collection))

    json.dump({'streams' : streams}, sys.stdout, indent=2)


def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def row_to_singer_record(stream, row, version, time_extracted):
    row_to_persist = {}

    for column_name, value in row.items():
        if isinstance(value, objectid.ObjectId):
            row_to_persist[column_name] = str(value)
        else:
            row_to_persist[column_name] = value

    return singer.RecordMessage(
        stream=stream['stream'],
        record=row_to_persist,
        version=version,
        time_extracted=time_extracted)


def is_stream_selected(stream):
    mdata = metadata.to_map(stream['metadata'])
    stream_metadata = mdata.get(())

    is_selected = stream_metadata.get('selected')

    return is_selected == True


def get_database_name(stream):
    md_map = metadata.to_map(stream['metadata'])

    return md_map.get((), {}).get('database-name')

def get_non_binlog_streams(client, streams, state):
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
        elif stream_state and replication_method == 'LOG_BASED' and binlog_stream_requires_historical(stream, state):
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
            lambda s: s.tap_stream_id == currently_syncing and is_valid_currently_syncing_stream(s, state),
            streams_with_state))

        non_currently_syncing_streams = list(filter(lambda s: s.tap_stream_id != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        # prioritize streams that have not been processed
        streams_to_sync = ordered_streams

    return streams_to_sync


def get_binlog_streams(client, streams, state):
    1


def sync_binlog_streams(client, streams, state):
    1


def do_sync_full_table(client, stream, state, columns):
    mdata = metadata.to_map(stream['metadata'])
    stream_metadata = mdata.get(())

    db = client[stream_metadata['database-name']]
    collection = db[stream['stream']]

    singer.write_message(singer.SchemaMessage(
        stream=stream['stream'],
        schema=stream['schema'],
        key_properties=['_id']))

    with metrics.record_counter(None) as counter:
        with collection.find() as cursor:
            rows_saved = 0

            time_extracted = utils.now()

            for row in cursor:
                rows_saved += 1
                whitelisted_row = {k:v for k,v in row.items() if k in columns}
                record_message = row_to_singer_record(stream,
                                                      whitelisted_row,
                                                      get_stream_version(stream['tap_stream_id'], state),
                                                      time_extracted)

                singer.write_message(record_message)

                if rows_saved % 1000 == 0:
                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def sync_non_binlog_streams(client, streams, state):
    for stream in streams:
        md_map = metadata.to_map(stream['metadata'])
        stream_metadata = md_map.get(())
        select_clause = stream_metadata.get('custom-select-clause')

        if not select_clause:
            LOGGER.warning('There are no columns selected for stream %s, skipping it.', stream['tap_stream_stream'])
            continue

        columns = [c.strip(' ') for c in select_clause.split(',')]
        columns.append('_id')

        state = singer.set_currently_syncing(state, stream['tap_stream_id'])

        # Emit a state message to indicate that we've started this stream
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        replication_method = stream_metadata.get('replication-method')

        database_name = get_database_name(stream)

        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = database_name
            timer.tags['table'] = stream['table_name']

            if replication_method == 'LOG_BASED':
                do_sync_historical_binlog(client, stream, state, columns)
            elif replication_method == 'FULL_TABLE':
                do_sync_full_table(client, stream, state, columns)
            else:
                raise Exception("only LOG_BASED and FULL TABLE replication methods are supported")

    state = singer.set_currently_syncing(state, None)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync(client, properties, state):
    all_streams = properties['streams']
    non_binlog_streams = get_non_binlog_streams(client, all_streams, state)
    binlog_streams = get_binlog_streams(client, all_streams, state)

    sync_non_binlog_streams(client, non_binlog_streams, state)
    sync_binlog_streams(client, binlog_streams, state)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    client =  MongoClient(host=config['host'],
                          port=int(config['port']),
                          username=config['user'],
                          password=config['password'],
                          authSource=config['dbname'])

    if args.discover:
         do_discover(client)
    elif args.properties:
        state = args.state or {}
        do_sync(client, args.properties, state)
    else:
        LOGGER.info("No properties were selected")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
