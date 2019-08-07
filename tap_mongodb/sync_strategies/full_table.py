#!/usr/bin/env python3
import time
from bson import objectid
import copy
import pymongo
import singer
from singer import metadata, metrics, utils
import tap_mongodb.sync_strategies.common as common


LOGGER = singer.get_logger()

def generate_bookmark_keys(stream):
    md_map = metadata.to_map(stream['metadata'])
    stream_metadata = md_map.get((), {})
    replication_method = stream_metadata.get('replication-method')

    base_bookmark_keys = {'last_id_fetched', 'max_id_value', 'version', 'initial_full_table_complete'}

    if replication_method == 'FULL_TABLE':
        bookmark_keys = base_bookmark_keys
    else:
        bookmark_keys = base_bookmark_keys.union(oplog.BOOKMARK_KEYS)

    return bookmark_keys


def get_max_id_value(collection):
    row = collection.find_one(sort=[("_id", pymongo.DESCENDING)])
    return str(row['_id'])


def sync_table(client, stream, state, projection):
    common.whitelist_bookmark_keys(generate_bookmark_keys(stream), stream['tap_stream_id'], state)
    mdata = metadata.to_map(stream['metadata'])
    stream_metadata = mdata.get(())

    database_name = stream_metadata['database-name']

    db = client[database_name]
    collection = db[stream['stream']]

    #before writing the table version to state, check if we had one to begin with
    first_run = singer.get_bookmark(state, stream['tap_stream_id'], 'version') is None

    # last run was interrupted if there is a last_id_fetched bookmark
    was_interrupted = singer.get_bookmark(state,
                                          stream['tap_stream_id'],
                                          'last_id_fetched') is not None

    #pick a new table version if last run wasn't interrupted
    if was_interrupted:
        stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    else:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    activate_version_message = singer.ActivateVersionMessage(
        stream=common.calculate_destination_stream_name(stream),
        version=stream_version
    )


    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if first_run:
        singer.write_message(activate_version_message)

    max_id_value = singer.get_bookmark(state,
                                        stream['tap_stream_id'],
                                        'max_id_value') or get_max_id_value(collection)

    last_id_fetched = singer.get_bookmark(state,
                                          stream['tap_stream_id'],
                                          'last_id_fetched')

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'max_id_value',
                                  max_id_value)


    find_filter = {'$lte': objectid.ObjectId(max_id_value)}

    if last_id_fetched:
        find_filter['$gte'] = objectid.ObjectId(last_id_fetched)

    LOGGER.info("Starting full table replication for table {}.{}".format(database_name, stream['stream']))

    with metrics.record_counter(None) as counter:
        with collection.find({'_id': find_filter},
                             projection,
                             sort=[("_id", pymongo.ASCENDING)]) as cursor:
            rows_saved = 0

            time_extracted = utils.now()

            for row in cursor:
                rows_saved += 1

                record_message = common.row_to_singer_record(stream,
                                                             row,
                                                             stream_version,
                                                             time_extracted)

                singer.write_message(record_message)

                state = singer.write_bookmark(state,
                                              stream['tap_stream_id'],
                                              'last_id_fetched',
                                              str(row['_id']))


                if rows_saved % 1000 == 0:
                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    # clear max pk value and last pk fetched upon successful sync
    singer.clear_bookmark(state, stream['tap_stream_id'], 'max_id_value')
    singer.clear_bookmark(state, stream['tap_stream_id'], 'last_id_fetched')

    singer.write_message(activate_version_message)
