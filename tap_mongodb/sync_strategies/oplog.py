#!/usr/bin/env python3
from bson import objectid, timestamp
import copy
import pymongo
import singer
from singer import metadata, metrics, utils

import tap_mongodb.sync_strategies.common as common

LOGGER = singer.get_logger()

SDC_DELETED_AT = "_sdc_deleted_at"

UPDATE_BOOKMARK_PERIOD = 1000

BOOKMARK_KEYS = {'oplog_ts_time', 'oplog_ts_inc', 'version'}

def get_latest_ts(client):
    row = client.local.oplog.rs.find_one(sort=[('$natural', pymongo.DESCENDING)])

    return row.get('ts')

def generate_streams_map(streams):
    streams_map = {}

    for stream in streams:
        md_map = metadata.to_map(stream['metadata'])
        stream_metadata = md_map.get(())
        select_clause = stream_metadata.get('custom-select-clause')

        if not select_clause:
            LOGGER.warning('There are no columns selected for stream %s, skipping it.', stream['tap_stream_stream'])
            continue

        columns = [c.strip(' ') for c in select_clause.split(',')]
        columns.append('_id')

        streams_map[stream['tap_stream_id']] = {
            'columns': columns,
            'stream': stream
            }

    return streams_map

def generate_tap_stream_id_for_row(row):
    row_db, row_table = row['ns'].split('.')

    return "{}-{}".format(row_db, row_table)

def update_bookmarks(state, streams_map, ts):
    for tap_stream_id in streams_map.keys():
        state = singer.write_bookmark(state,
                                      tap_stream_id,
                                      'oplog_ts_time',
                                      ts.time)

        state = singer.write_bookmark(state,
                                      tap_stream_id,
                                      'oplog_ts_inc',
                                      ts.inc)

    return state


def sync_oplog_stream(client, streams, state):
    streams_map = generate_streams_map(streams)

    for tap_stream_id in streams_map.keys():
        common.whitelist_bookmark_keys(BOOKMARK_KEYS, tap_stream_id, state)

    for tap_stream_id, bookmark in state.get('bookmarks', {}).items():
        columns = streams_map.get(tap_stream_id)

        if not columns:
            continue

        oplog_ts = min([timestamp.Timestamp(v['oplog_ts_time'], v['oplog_ts_inc'])
                        for k,v in state.get('bookmarks', {}).items()
                        if streams_map.get(k)])

        LOGGER.info("Starting oplog replication with ts=%s", oplog_ts)

        time_extracted = utils.now()

        rows_saved = 0
        ops_skipped = 0

        with client.local.oplog.rs.find({'ts': {'$gt': oplog_ts}},
                                        oplog_replay=True) as cursor:
            for row in cursor:
                if row['op'] == 'n':
                    LOGGER.info('Skipping noop op')
                elif not streams_map.get(generate_tap_stream_id_for_row(row)):
                    ops_skipped = ops_skipped + 1

                    if ops_skipped % UPDATE_BOOKMARK_PERIOD == 0:
                        LOGGER.info("Skipped %s ops so far as they were not for selected tables; %s rows extracted",
                                    ops_skipped,
                                    rows_saved)
                else:
                    row_op = row['op']
                    if row_op in ['i', 'u']:
                        tap_stream_id = generate_tap_stream_id_for_row(row)
                        stream_map_entry = streams_map[tap_stream_id]
                        whitelisted_row = {k:v for k,v in row['o'].items() if k in stream_map_entry['columns']}
                        record_message = common.row_to_singer_record(stream_map_entry['stream'],
                                                                     whitelisted_row,
                                                                     common.get_stream_version(tap_stream_id, state),
                                                                     time_extracted)

                        singer.write_message(record_message)

                    elif row_op == 'd':
                        tap_stream_id = generate_tap_stream_id_for_row(row)
                        stream_map_entry = streams_map[tap_stream_id]

                        # Delete ops only contain the _id of the row deleted
                        whitelisted_row = {column_name:None for column_name in stream_map_entry['columns']}

                        whitelisted_row['_id'] = row['o']['_id']
                        whitelisted_row[SDC_DELETED_AT] = row['ts']

                        record_message = common.row_to_singer_record(stream_map_entry['stream'],
                                                                     whitelisted_row,
                                                                     common.get_stream_version(tap_stream_id, state),
                                                                     time_extracted)
                        singer.write_message(record_message)
                    else:
                        LOGGER.info("Skipping op for table %s as it is not an INSERT, UPDATE, or DELETE", row['ns'])

                state = update_bookmarks(state,
                                         streams_map,
                                         row['ts'])
