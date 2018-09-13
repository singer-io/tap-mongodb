#!/usr/bin/env python3
from bson import objectid
import copy
import pymongo
import singer
from singer import metadata, metrics, utils

LOGGER = singer.get_logger()

SDC_DELETED_AT = "_sdc_deleted_at"

UPDATE_BOOKMARK_PERIOD = 1000

BOOKMARK_KEYS = {'oplog_h', 'version'}

def get_latest_h(client):
    row = client.local.oplog.rs.find_one(sort=[('$natural', pymongo.DESCENDING)])

    return row.get('h')

def generate_streams_map(binlog_streams):
    stream_map = {}

    for catalog_entry in binlog_streams:
        columns = add_automatic_properties(catalog_entry,
                                           list(catalog_entry.schema.properties.keys()))

        stream_map[catalog_entry.tap_stream_id] = {
            'catalog_entry': catalog_entry,
            'desired_columns': columns
        }

    return stream_map

def sync_oplog_stream(client, streams, state):
    streams_map = generate_streams_map(binlog_streams)

    for tap_stream_id in streams_map.keys():
        common.whitelist_bookmark_keys(BOOKMARK_KEYS, tap_stream_id, state)
