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
IGNORE_COLLECTIONS = ['system.indexes']


def produce_collection_schema(collection):
    collection_name = collection.name
    collection_db_name = collection.database.name
    mdata = {}
    mdata = metadata.write(mdata, (), 'database-name', collection_db_name)
    mdata = metadata.write(mdata, (), 'row-count', collection.estimated_document_count())

    # Get indexes
    coll_indexes = collection.index_information()
    if coll_indexes.get('_id_'):
        mdata = metadata.write(mdata, (), 'table-key-properties', ['_id'])

    


    
    # If _id is in indexes, write `table-key-properties = ['_id']` metadata
    # If _id isn't present, look for indexes with unique=True, and write that as table-key-properties

    # Look for any indexes that aren't _id, and write them as 'valid-replication-key=[]' metadata
    
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



def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    client =  MongoClient(host=config['host'],
                          port=int(config['port']),
                          username=config.get('user', None),
                          password=config.get('password', None),
                          authSource=config['dbname'])


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
