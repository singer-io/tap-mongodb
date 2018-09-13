#!/usr/bin/env python3
from bson import objectid, timestamp
import singer

def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    for bk in [non_whitelisted_bookmark_key
               for non_whitelisted_bookmark_key
               in state.get('bookmarks', {}).get(tap_stream_id, {}).keys()
               if non_whitelisted_bookmark_key not in bookmark_key_set]:
        singer.clear_bookmark(state, tap_stream_id, bk)


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
        elif isinstance(value, timestamp.Timestamp):
            row_to_persist[column_name] = value.as_datetime().isoformat()
        else:
            row_to_persist[column_name] = value

    return singer.RecordMessage(
        stream=stream['stream'],
        record=row_to_persist,
        version=version,
        time_extracted=time_extracted)
