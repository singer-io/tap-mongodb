#!/usr/bin/env python3
import base64
import datetime
import time
import uuid
import bson
from bson import objectid, timestamp, datetime as bson_datetime
import singer
from singer import utils, metadata
from terminaltables import AsciiTable

import pytz
import tzlocal

INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME = False
UPDATE_BOOKMARK_PERIOD = 1000
COUNTS = {}
TIMES = {}

class InvalidProjectionException(Exception):
    """Raised if projection blacklists _id"""

class UnsupportedReplicationKeyTypeException(Exception):
    """Raised if key type is unsupported"""

class MongoAssertionException(Exception):
    """Raised if Mongo exhibits incorrect behavior"""

class MongoInvalidDateTimeException(Exception):
    """Raised if we find an invalid date-time that we can't handle"""

def calculate_destination_stream_name(stream):
    s_md = metadata.to_map(stream['metadata'])
    if INCLUDE_SCHEMAS_IN_DESTINATION_STREAM_NAME:
        return "{}_{}".format(s_md.get((), {}).get('database-name'), stream['stream'])

    return stream['stream']

def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    for bookmark_key in [non_whitelisted_bookmark_key
                         for non_whitelisted_bookmark_key
                         in state.get('bookmarks', {}).get(tap_stream_id, {}).keys()
                         if non_whitelisted_bookmark_key not in bookmark_key_set]:
        singer.clear_bookmark(state, tap_stream_id, bookmark_key)


def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version

def class_to_string(bookmark_value, bookmark_type):
    if bookmark_type == 'datetime':
        timezone = tzlocal.get_localzone()
        local_datetime = timezone.localize(bookmark_value)
        utc_datetime = local_datetime.astimezone(pytz.UTC)
        return utils.strftime(utc_datetime)
    if bookmark_type == 'Timestamp':
        return '{}.{}'.format(bookmark_value.time, bookmark_value.inc)
    if bookmark_type == 'bytes':
        return base64.b64encode(bookmark_value).decode('utf-8')
    if bookmark_type in ['int', 'Int64', 'float', 'ObjectId', 'str', 'UUID']:
        return str(bookmark_value)
    raise UnsupportedReplicationKeyTypeException("{} is not a supported replication key type"
                                                 .format(bookmark_type))


# pylint: disable=too-many-return-statements
def string_to_class(str_value, type_value):
    if type_value == 'UUID':
        return uuid.UUID(str_value)
    if type_value == 'datetime':
        return singer.utils.strptime_with_tz(str_value)
    if type_value == 'int':
        return int(str_value)
    if type_value == 'Int64':
        return bson.int64.Int64(str_value)
    if type_value == 'float':
        return float(str_value)
    if type_value == 'ObjectId':
        return objectid.ObjectId(str_value)
    if type_value == 'Timestamp':
        split_value = str_value.split('.')
        return bson.timestamp.Timestamp(int(split_value[0]), int(split_value[1]))
    if type_value == 'bytes':
        return base64.b64decode(str_value.encode())
    if type_value == 'str':
        return str(str_value)
    raise UnsupportedReplicationKeyTypeException("{} is not a supported replication key type"
                                                 .format(type_value))

def safe_transform_datetime(value, path):
    timezone = tzlocal.get_localzone()
    try:
        local_datetime = timezone.localize(value)
        utc_datetime = local_datetime.astimezone(pytz.UTC)
    except Exception as ex:
        if str(ex) == "year is out of range" and value.year == 0:
            # NB: Since datetimes are persisted as strings, it doesn't
            # make sense to blow up on invalid Python datetimes (e.g.,
            # year=0). In this case we're formatting it as a string and
            # passing it along down the pipeline.
            return "{:04d}-{:02d}-{:02d}T{:02d}:{:02d}:{:02d}.{:06d}Z".format(value.year,
                                                                              value.month,
                                                                              value.day,
                                                                              value.hour,
                                                                              value.minute,
                                                                              value.second,
                                                                              value.microsecond)
        raise MongoInvalidDateTimeException("Found invalid datetime at [{}]: {}".format(
            ".".join(map(str, path)),
            value))
    return utils.strftime(utc_datetime)

# pylint: disable=too-many-return-statements,too-many-branches
def transform_value(value, path):
    if isinstance(value, list):
        # pylint: disable=unnecessary-lambda
        return list(map(lambda v: transform_value(v[1], path + [v[0]]), enumerate(value)))
    if isinstance(value, dict):
        return {k:transform_value(v, path + [k]) for k, v in value.items()}
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, objectid.ObjectId):
        return str(value)
    if isinstance(value, bson_datetime.datetime):
        return safe_transform_datetime(value, path)
    if isinstance(value, timestamp.Timestamp):
        return utils.strftime(value.as_datetime())
    if isinstance(value, bson.int64.Int64):
        return int(value)
    if isinstance(value, bytes):
        # Return the original base64 encoded string
        return base64.b64encode(value).decode('utf-8')
    if isinstance(value, datetime.datetime):
        timezone = tzlocal.get_localzone()
        local_datetime = timezone.localize(value)
        utc_datetime = local_datetime.astimezone(pytz.UTC)
        return utils.strftime(utc_datetime)
    if isinstance(value, bson.decimal128.Decimal128):
        return value.to_decimal()
    if isinstance(value, bson.code.Code):
        if value.scope:
            return {
                'value': str(value),
                'scope': str(value.scope)
            }
        return str(value)
    if isinstance(value, bson.regex.Regex):
        return {
            'pattern': value.pattern,
            'flags': value.flags
        }
    if isinstance(value, bson.dbref.DBRef):
        return {
            'id': str(value.id),
            'collection': value.collection,
            'database': value.database
        }

    return value


def row_to_singer_record(stream, row, version, time_extracted):
    # pylint: disable=unidiomatic-typecheck
    try:
        row_to_persist = {k:transform_value(v, [k]) for k, v in row.items()
                          if type(v) not in [bson.min_key.MinKey, bson.max_key.MaxKey]}
    except MongoInvalidDateTimeException as ex:
        raise Exception("Error syncing collection {}, object ID {} - {}".format(stream["tap_stream_id"], row['_id'], ex))

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
        md_map = metadata.to_map(stream['metadata'])
        db_name = metadata.get(md_map, (), 'database-name')
        replication_method = metadata.get(md_map, (), 'replication-method')

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
    table = AsciiTable(data, title='Sync Summary')

    return '\n\n' + table.table
