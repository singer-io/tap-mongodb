#!/usr/bin/env python3
from bson import objectid, timestamp
import copy
import pymongo
import time
import singer
from singer import metadata, metrics, utils
import tap_mongodb.sync_strategies.common as common

LOGGER = singer.get_logger()

SDC_DELETED_AT = "_sdc_deleted_at"
MAX_UPDATE_BUFFER_LENGTH = 500

def get_latest_collection_ts(client, stream):
    md_map = metadata.to_map(stream['metadata'])
    stream_metadata = md_map.get(())
    db_name = stream_metadata.get("database-name")
    collection_name = stream.get("table_name")

    find_query = {'ns': '{}.{}'.format(db_name, collection_name)}
    row = client.local.oplog.rs.find_one(find_query,
                                         sort=[('$natural', pymongo.DESCENDING)])
    return row.get('ts')

def oplog_has_aged_out(client, state, stream):
    md_map = metadata.to_map(stream['metadata'])
    stream_metadata = md_map.get(())
    db_name = stream_metadata.get("database-name")
    collection_name = stream.get("table_name")

    find_query = {'ns': '{}.{}'.format(db_name, collection_name)}
    earliest_ts_row = client.local.oplog.rs.find_one(find_query,
                                                     sort=[('$natural', pymongo.ASCENDING)])
    earliest_ts = earliest_ts_row.get('ts')

    stream_state = state.get('bookmarks', {}).get(stream['tap_stream_id'])
    bookmarked_ts = timestamp.Timestamp(stream_state['oplog_ts_time'],
                                        stream_state['oplog_ts_inc'])

    return bookmarked_ts < earliest_ts

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
    base_projection = {
        "ts": 1, "ns": 1, "op": 1, 'o2': 1
    }
    new_projection = {}


    # If no projection was provided, return base_projection with 'o' whitelisted
    if not projection:
        new_projection = base_projection
        new_projection['o'] = 1
        return new_projection

    temp_projection = {k:v for k,v in projection.items() if k!='_id'}
    is_whitelist = sum([v for k,v in temp_projection.items()]) > 0

    # If only '_id' is included in projection
    if len(temp_projection)==0:

        # If only '_id' is whitelisted, return base projection with 'o._id' whitelisted
        if projection['_id'] == 1:
            new_projection = base_projection
            new_projection['o._id'] = 1
            return new_projection
        # If only '_id' is blacklisted, return base projection with 'o' whitelisted
        else:
            new_projection = base_projection
            new_projection['o'] = 1
            return new_projection

    # If whitelist is provided, return base projection along with whitelisted fields and whitelisted id
    if is_whitelist:
        new_projection = base_projection
        for field,value in temp_projection.items():
            new_projection['o.'+field] = value
        new_projection['o._id'] = 1
        return new_projection
    # If blacklist is provided, return blacklisted fields with _id whitelisted
    else:
        for field,value in temp_projection.items():
            new_projection['o.'+field] = value
        return new_projection


def flush_buffer(client, update_buffer, stream_projection, db_name, collection_name):
    query = {'_id': {'$in': list(update_buffer)}}
    with client[db_name][collection_name].find(query, stream_projection) as cursor:
        for row in cursor:
            yield row


def sync_collection(client, stream, state, stream_projection):
    tap_stream_id = stream['tap_stream_id']
    LOGGER.info('Starting oplog sync for {}'.format(tap_stream_id))

    md_map = metadata.to_map(stream['metadata'])
    stream_metadata = md_map.get(())
    database_name = stream_metadata.get("database-name")
    collection_name = stream.get("table_name")
    stream_state = state.get('bookmarks', {}).get(tap_stream_id)

    oplog_ts = timestamp.Timestamp(stream_state['oplog_ts_time'],
                                   stream_state['oplog_ts_inc'])

    # Write activate version message
    version = common.get_stream_version(tap_stream_id, state)
    activate_version_message = singer.ActivateVersionMessage(
        stream=common.calculate_destination_stream_name(stream),
        version=version
    )
    singer.write_message(activate_version_message)

    time_extracted = utils.now()
    rows_saved = 0
    start_time = time.time()

    oplog_query = {
        'ts': {'$gte': oplog_ts},
        'op': {'$in': ['i', 'u', 'd']},
        'ns': '{}.{}'.format(database_name, collection_name)
    }

    projection = transform_projection(stream_projection)

    # _id is the key-property so don't let it get turned off
    if stream_projection and stream_projection.get('_id') == 0:
        stream_projection.pop('_id')
    if stream_projection and len(stream_projection) == 0:
        stream_projection = None

    LOGGER.info('Querying {} with:\n\tFind Parameters: {}\n\tProjection: {}'.format(
        tap_stream_id, oplog_query, projection))

    update_buffer = set()

    with client.local.oplog.rs.find(oplog_query, projection) as cursor:
        for row in cursor:
            row_op = row['op']
            if row_op == 'i':

                record_message = common.row_to_singer_record(stream, row['o'], version, time_extracted)
                singer.write_message(record_message)

                rows_saved += 1

            elif row_op == 'u':
                update_buffer.add(row['o2']['_id'])

            elif row_op == 'd':

                # remove update from buffer if that document has been deleted
                if row['o']['_id'] in update_buffer:
                    update_buffer.remove(row['o']['_id'])

                # Delete ops only contain the _id of the row deleted
                row['o'][SDC_DELETED_AT] = row['ts']

                record_message = common.row_to_singer_record(stream, row['o'], version, time_extracted)
                singer.write_message(record_message)

                rows_saved += 1
            else:
                LOGGER.info("Skipping op for table %s as it is not an INSERT, UPDATE, or DELETE", row['ns'])

            state = update_bookmarks(state,
                                     tap_stream_id,
                                     row['ts'])

            # flush buffer if it has filled up
            if len(update_buffer) >= MAX_UPDATE_BUFFER_LENGTH:
                for row in flush_buffer(client, update_buffer, stream_projection, database_name, collection_name):
                    record_message = common.row_to_singer_record(stream, row, version, time_extracted)
                    singer.write_message(record_message)

                    rows_saved += 1
                update_buffer = set()

            # write state every UPDATE_BOOKMARK_PERIOD messages
            if rows_saved % common.UPDATE_BOOKMARK_PERIOD == 0:
                # flush buffer before writing state
                for row in flush_buffer(client, update_buffer, stream_projection, database_name, collection_name):
                    record_message = common.row_to_singer_record(stream, row, version, time_extracted)
                    singer.write_message(record_message)

                    rows_saved += 1
                update_buffer = set()

                # write state
                singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        # flush buffer if finished with oplog
        for row in flush_buffer(client, update_buffer, stream_projection, database_name, collection_name):
            record_message = common.row_to_singer_record(stream, row, version, time_extracted)

            singer.write_message(record_message)
            rows_saved += 1

    common.COUNTS[tap_stream_id] += rows_saved
    common.TIMES[tap_stream_id] += time.time()-start_time
    LOGGER.info('Syncd {} records for {}'.format(rows_saved, tap_stream_id))
