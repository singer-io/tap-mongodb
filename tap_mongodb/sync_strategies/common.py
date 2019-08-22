#!/usr/bin/env python3
import datetime
import bson
from bson import objectid, timestamp, datetime as bson_datetime
import singer
from singer import utils, metadata
from terminaltables import AsciiTable

import pytz
import time
import tzlocal
import decimal

include_schemas_in_destination_stream_name = False
UPDATE_BOOKMARK_PERIOD = 1000
COUNTS = {}
TIMES = {}

def calculate_destination_stream_name(stream):
    s_md = metadata.to_map(stream['metadata'])
    if include_schemas_in_destination_stream_name:
        return "{}_{}".format(s_md.get((), {}).get('database-name'), stream['stream'])

    return stream['stream']

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


def transform_value(value):
    if isinstance(value, list):
        return list(map(lambda v: transform_value(v), value))
    elif isinstance(value, dict):
        return {k:transform_value(v) for k,v in value.items()}
    elif isinstance(value, objectid.ObjectId):
        return str(value)
    elif isinstance(value, bson_datetime.datetime):
        timezone = tzlocal.get_localzone()
        local_datetime = timezone.localize(value)
        utc_datetime = local_datetime.astimezone(pytz.UTC)
        return utils.strftime(utc_datetime)
    elif isinstance(value, timestamp.Timestamp):
        return utils.strftime(value.as_datetime())
    elif isinstance(value, bson.int64.Int64):
        return int(value)
    elif isinstance(value, bytes):
        return value.decode()
    elif isinstance(value, datetime.datetime):
        timezone = tzlocal.get_localzone()
        local_datetime = timezone.localize(value)
        utc_datetime = local_datetime.astimezone(pytz.UTC)
        return utils.strftime(utc_datetime)
    elif isinstance(value, bson.decimal128.Decimal128):
        return value.to_decimal()
    elif isinstance(value, bson.code.Code):
        if value.scope:
            return {
                'value': str(value),
                'scope': str(value.scope)
            }
        return str(value)
    elif isinstance(value, bson.regex.Regex):
        return {
            'pattern': value.pattern,
            'flags': value.flags
        }
    else:
        return value


def row_to_singer_record(stream, row, version, time_extracted):
    row_to_persist = {k:transform_value(v) for k,v in row.items() if type(v) not in [bson.min_key.MinKey, bson.max_key.MaxKey]}

    return singer.RecordMessage(
        stream=calculate_destination_stream_name(stream),
        record=row_to_persist,
        version=version,
        time_extracted=time_extracted)

def get_sync_summary(catalog):
    headers = [['database',
                'collection',
                'replication method',
                'total records',
                'write speed']]

    rows = []
    for stream_id, stream_count in COUNTS.items():
        stream = [x for x in catalog['streams'] if x['tap_stream_id'] == stream_id][0]
        collection_name = stream.get("table_name")
        stream_metadata = metadata.to_map(stream['metadata']).get(())
        db_name = stream_metadata.get("database-name")
        replication_method = stream_metadata.get('replication-method')

        stream_time = TIMES[stream_id]
        if stream_time == 0:
            stream_time = 0.000001
        row = [db_name,
               collection_name,
               replication_method,
               '{} records'.format(stream_count),
               '{:.1f} records/second'.format(stream_count/stream_time)]
        rows.append(row)

    data = headers + rows
    table = AsciiTable(data, title = 'Sync Summary')

    return '\n\n' + table.table
