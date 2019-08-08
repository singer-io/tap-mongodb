#!/usr/bin/env python3
from bson import objectid, timestamp
import copy
import pymongo
import singer
from singer import metadata, metrics, utils

import tap_mongodb.sync_strategies.common as common

LOGGER = singer.get_logger()

SDC_DELETED_AT = "_sdc_deleted_at"

BOOKMARK_KEYS = {'oplog_ts_time', 'oplog_ts_inc', 'version'}

def get_latest_ts(client):
    row = client.local.oplog.rs.find_one(sort=[('$natural', pymongo.DESCENDING)])

    return row.get('ts')

def update_bookmarks(state, tap_stream_id, ts):
    state = singer.write_bookmark(state,
                                  tap_stream_id,
                                  'oplog_ts_time',
                                  ts.time)

    state = singer.write_bookmark(state,
                                  tap_stream_id,
                                  'oplog_ts_inc',
                                  ts.inc)

    return state


def transform_projection(projection):
    new_projection = {}
    for field,value in projection.items():
        new_projection['o.'+field] = value
    new_projection['o._id'] = 1
    return new_projection

def sync_oplog_stream(client, stream, state, stream_projection):
    tap_stream_id = stream['tap_stream_id']
    md_map = metadata.to_map(stream['metadata'])
    stream_metadata = md_map.get(())
    db_name = stream_metadata.get("database-name")
    collection_name = stream.get("table_name")

    common.whitelist_bookmark_keys(BOOKMARK_KEYS, tap_stream_id, state)

    oplog_ts = min([timestamp.Timestamp(v['oplog_ts_time'], v['oplog_ts_inc'])
                    for k,v in state.get('bookmarks', {}).items()
                    if streams_map.get(k)])

    LOGGER.info("Starting oplog replication with ts=%s", oplog_ts)

    time_extracted = utils.now()

    rows_saved = 0
    ops_skipped = 0

    oplog_query = {
        'ts': {'$gt': oplog_ts},
        'op': {'$in': ['i', 'u', 'd']},
        'ns': '{}.{}'.format(db_name, collection_name)
    }

    base_projection = {
        "ts": 1, "ns": 1, "op": 1
    }
    if stream_projection:
        projection = base_projection.update(transform_projection(stream_projection))
    else:
        projection = base_projection
        projection['o'] = 1

    with client.local.oplog.rs.find(oplog_query, projection, oplog_replay=True) as cursor:
        for row in cursor:
            row_op = row['op']
            if row_op in ['i', 'u']:
                record_message = common.row_to_singer_record(stream,
                                                             row['o'],
                                                             common.get_stream_version(tap_stream_id, state),
                                                             time_extracted)
                rows_saved += 1

                singer.write_message(record_message)

            elif row_op == 'd':
                # Delete ops only contain the _id of the row deleted
                whitelisted_row['_id'] = row['o']['_id']
                whitelisted_row[SDC_DELETED_AT] = row['ts']

                record_message = common.row_to_singer_record(stream,
                                                             whitelisted_row,
                                                             common.get_stream_version(tap_stream_id, state),
                                                             time_extracted)
                singer.write_message(record_message)
                rows_saved += 1
            else:
                LOGGER.info("Skipping op for table %s as it is not an INSERT, UPDATE, or DELETE", row['ns'])

            state = update_bookmarks(state,
                                     tap_stream_id,
                                     row['ts'])
            if rows_saved % common.UPDATE_BOOKMARK_PERIOD == 0:
                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
