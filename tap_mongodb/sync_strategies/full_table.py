#!/usr/bin/env python3
from bson import objectid
import copy
import pymongo
import singer
from singer import metadata, metrics, utils
import tap_mongodb.sync_strategies.common as common
import tap_mongodb.sync_strategies.oplog as oplog

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


def sync_table(client, stream, state, stream_version, columns):
    common.whitelist_bookmark_keys(generate_bookmark_keys(stream), stream['tap_stream_id'], state)

    mdata = metadata.to_map(stream['metadata'])
    stream_metadata = mdata.get(())

    database_name = stream_metadata['database-name']

    db = client[database_name]
    collection = db[stream['stream']]

    activate_version_message = singer.ActivateVersionMessage(
        stream=stream['stream'],
        version=stream_version
    )

    initial_full_table_complete = singer.get_bookmark(state,
                                                      stream['tap_stream_id'],
                                                      'initial_full_table_complete')

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if not initial_full_table_complete:
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
        find_filter['$gt': objectid.ObjectId(last_id_fetched)]

    LOGGER.info("Starting full table replication for table {}.{}".format(database_name, stream['stream']))

    with metrics.record_counter(None) as counter:
        with collection.find({'_id': find_filter},
                             sort=[("_id", pymongo.DESCENDING)]) as cursor:
            rows_saved = 0

            time_extracted = utils.now()

            for row in cursor:
                rows_saved += 1

                whitelisted_row = {k:v for k,v in row.items() if k in columns}
                record_message = common.row_to_singer_record(stream,
                                                             whitelisted_row,
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
