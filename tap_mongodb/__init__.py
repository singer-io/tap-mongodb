#!/usr/bin/env python3
import copy
import json
import sys
from pymongo import MongoClient


import singer
from singer import metadata, metrics, utils

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
    database_names = client.list_database_names()

    streams = []

    for db_name in [d for d in database_names
                    if d not in IGNORE_DBS]:
        db = client[db_name]


        collection_names = db.list_collection_names()
        collection_names_filtered = [c for c in collection_names if not c.startswith("system.")]

        for collection_name in [c for c in collection_names
                                if not c.startswith("system.")]:

            LOGGER.info("Getting collection info for db: " + db_name + ", collection: " + collection_name)
            
            collection = db[collection_name]

            streams.append(produce_collection_schema(collection))

    json.dump({'streams' : streams}, sys.stdout, indent=2)



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
    else:
        LOGGER.info("Only discovery mode supported right now")


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
