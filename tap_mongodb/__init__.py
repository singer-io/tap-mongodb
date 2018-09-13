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

    import pdb
    pdb.set_trace
    1 + 1
    for column_name, value in row.items():
        if isinstance(value, objectid.ObjectId):
            row_to_persist[column_name] = str(value)
        else:
            row_to_persist[column_name] = value

    return singer.RecordMessage(
        stream=stream['stream'],
        record=row_to_persist,
        version=version,
        time_extracted=time_extracted
    )


def do_sync(client, properties, state):
    streams = properties['streams']

    for stream in streams:
        mdata = metadata.to_map(stream['metadata'])
        stream_metadata = mdata.get(())

        select_clause = stream_metadata.get('custom-select-clause')

        if not stream_metadata['selected']:
            LOGGER.info("Skipping stream {} since it was not selected.".format(stream['tap_stream_id']))
            continue

        if not select_clause:
            LOGGER.info("Skipping stream {} since no select clause was provided.".format(stream['tap_stream_id']))
            continue

        columns = [c.strip(' ') for c in select_clause.split(',')]
        columns.append('_id')

        db = client[stream_metadata['database-name']]
        collection = db[stream['stream']]

        singer.write_message(singer.SchemaMessage(
            stream=stream['stream'],
            schema=stream['schema'],
            key_properties=['_id']
        ))

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
    # db = client.test
    # foo = db.foo
    # print(foo.find_one())

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
