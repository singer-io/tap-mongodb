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


def transform_list(list_value):
    return list(map(lambda v: transform_value(v), list_value))


def transform_dict(dict_value):
    return {k:transform_value(v) for k,v in dict_value.items()}


def transform_value(value):
    if isinstance(value, list):
        return transform_list(value)
    elif isinstance(value, dict):
        return transform_dict(value)
    elif isinstance(value, objectid.ObjectId):
        return str(value)
    elif isinstance(value, timestamp.Timestamp):
        return value.as_datetime().isoformat()
    else:
        return value


def row_to_singer_record(stream, row, version, time_extracted):
    row_to_persist = {k:transform_value(v) for k,v in row.items()}

    return singer.RecordMessage(
        stream=stream['stream'],
        record=row_to_persist,
        version=version,
        time_extracted=time_extracted)
