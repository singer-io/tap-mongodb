from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import datetime
import unittest
import datetime
import pymongo
import string
import random
import time
import re
import pprint
import pdb
import bson
from functools import reduce
from singer import utils, metadata
from mongodb_common import drop_all_collections
import decimal


def get_test_connection():
    username = os.getenv('TAP_MONGODB_USER')
    password = os.getenv('TAP_MONGODB_PASSWORD')
    host= os.getenv('TAP_MONGODB_HOST')
    auth_source = os.getenv('TAP_MONGODB_DBNAME')
    port = os.getenv('TAP_MONGODB_PORT')
    ssl = False
    conn = pymongo.MongoClient(host=host, username=username, password=password, port=27017, authSource=auth_source, ssl=ssl)
    return conn

def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def generate_simple_coll_docs(num_docs):
    docs = []
    for int_value in range(num_docs):
        docs.append({"int_field": int_value, "string_field": random_string_generator()})
    return docs

class MongoDBDiscovery(unittest.TestCase):
    def setUp(self):
        if not all([x for x in [os.getenv('TAP_MONGODB_HOST'),
                                    os.getenv('TAP_MONGODB_USER'),
                                    os.getenv('TAP_MONGODB_PASSWORD'),
                                    os.getenv('TAP_MONGODB_PORT'),
                                    os.getenv('TAP_MONGODB_DBNAME')]]):
            #pylint: disable=line-too-long
            raise Exception("set TAP_MONGODB_HOST, TAP_MONGODB_USER, TAP_MONGODB_PASSWORD, TAP_MONGODB_PORT, TAP_MONGODB_DBNAME")


        with get_test_connection() as client:
            # drop all dbs/collections
            drop_all_collections(client)

            # simple_coll_1 has 50 documents
            client["simple_db"]["simple_coll_1"].insert_many(generate_simple_coll_docs(50))

            # simple_coll_2 has 100 documents
            client["simple_db"]["simple_coll_2"].insert_many(generate_simple_coll_docs(100))

            # admin_coll_1 has 50 documents
            client["admin"]["admin_coll_1"].insert_many(generate_simple_coll_docs(50))

            # create view on simple_coll_1
            client["simple_db"].command(bson.son.SON([("create", "simple_view_1"), ("viewOn", "simple_coll_1"), ("pipeline", [])]))

            # collections with same names as others in different dbs
            client["simple_db_2"]["simple_coll_1"].insert_many(generate_simple_coll_docs(50))
            client["simple_db_2"]["SIMPLE_COLL_1"].insert_many(generate_simple_coll_docs(50))

            # collections with special characters in names
            client["special_db"]["hebrew_ישראל"].insert_many(generate_simple_coll_docs(50))
            client['special_db']['hello!world?'].insert_many(generate_simple_coll_docs(50))

            # Add datatype collections
            pattern = re.compile('.*')
            regex = bson.Regex.from_native(pattern)
            regex.flags ^= re.UNICODE
            datatype_doc = {
                "double_field": 4.3,
                "string_field": "a sample string",
                "object_field" : {
                    "obj_field_1_key": "obj_field_1_val",
                    "obj_field_2_key": "obj_field_2_val"
                },
                "array_field" : [
                    "array_item_1",
                    "array_item_2",
                    "array_item_3"
                ],
                "binary_data_field" : b"a binary string",
                "object_id_field": bson.objectid.ObjectId(b'123456789123'),
                "boolean_field" : True,
                "date_field" : datetime.datetime.now(),
                "null_field": None,
                "regex_field" : regex,
                "32_bit_integer_field" : 32,
                "timestamp_field" : bson.timestamp.Timestamp(int(time.time()), 1),
                "64_bit_integer_field" : 34359738368,
                "decimal_field" : bson.Decimal128(decimal.Decimal('1.34')),
                "javaScript_field" : bson.code.Code("var x, y, z;"),
                "javaScript_with_scope_field" : bson.code.Code("function incrementX() { x++; }", scope={"x": 1}),
                "min_key_field" : bson.min_key.MinKey,
                "max_key_field" : bson.max_key.MaxKey
            }
            client["datatype_db"]["datatype_coll_1"].insert_one(datatype_doc)

    def expected_check_streams(self):
        return {
            'simple_db-simple_coll_1',
            'simple_db-simple_coll_2',
            'simple_db_2-simple_coll_1',
            'simple_db_2-SIMPLE_COLL_1',
            'admin-admin_coll_1',
            #'simple_db-simple_view_1',
            'datatype_db-datatype_coll_1',
            'special_db-hebrew_ישראל',
            'special_db-hello!world?'
        }

    def expected_pks(self):
        return {
            'simple_db-simple_coll_1': {'_id'},
            'simple_db-simple_coll_2': {'_id'},
            'simple_db_2-simple_coll_1': {'_id'},
            'simple_db_2-SIMPLE_COLL_1': {'_id'},
            'admin-admin_coll_1': {'_id'},
            #'simple_db-simple_view_1': {'_id'},
            'datatype_db-datatype_coll_1': {'_id'},
            'special_db-hebrew_ישראל': {'_id'},
            'special_db-hello!world?': {'_id'}
        }

    def expected_row_counts(self):
        return {
            'simple_db-simple_coll_1': 50,
            'simple_db-simple_coll_2': 100,
            'simple_db_2-simple_coll_1': 50,
            'simple_db_2-SIMPLE_COLL_1': 50,
            'admin-admin_coll_1': 50,
            #'simple_db-simple_view_1': 50,
            'datatype_db-datatype_coll_1': 1,
            'special_db-hebrew_ישראל': 50,
            'special_db-hello!world?': 50
        }

    def expected_table_names(self):
        return {
            'simple_coll_1',
            'simple_coll_2',
            'SIMPLE_COLL_1',
            'admin_coll_1',
            'datatype_coll_1',
            'hebrew_ישראל',
            'hello!world?'
        }

    def name(self):
        return "tap_tester_mongodb_discovery"

    def tap_name(self):
        return "tap-mongodb"

    def get_type(self):
        return "platform.mongodb"

    def get_credentials(self):
        return {'password': os.getenv('TAP_MONGODB_PASSWORD')}

    def get_properties(self):
        return {'host' : os.getenv('TAP_MONGODB_HOST'),
                'port' : os.getenv('TAP_MONGODB_PORT'),
                'user' : os.getenv('TAP_MONGODB_USER'),
                'database' : os.getenv('TAP_MONGODB_DBNAME')
        }

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # tap discovered the right streams
        catalog = menagerie.get_catalog(conn_id)


        for stream in catalog['streams']:
            # schema is open {} for each stream
            self.assertEqual({'type': 'object'}, stream['schema'])

        # assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         {c['tap_stream_id'] for c in catalog['streams']})
        # Verify that the table_name is in the format <collection_name> for each stream
        self.assertEqual(self.expected_table_names(), {c['table_name'] for c in catalog['streams']})

        for tap_stream_id in self.expected_check_streams():
            found_stream = [c for c in catalog['streams'] if c['tap_stream_id'] == tap_stream_id][0]
            stream_metadata = [x['metadata'] for x in found_stream['metadata'] if x['breadcrumb']==[]][0]

            # table-key-properties metadata
            self.assertEqual(self.expected_pks()[tap_stream_id],
                             set(stream_metadata.get('table-key-properties')))

            # row-count metadata
            self.assertEqual(self.expected_row_counts()[tap_stream_id],
                             stream_metadata.get('row-count'))

            # selected metadata is None for all streams
            self.assertNotIn('selected', stream_metadata.keys())

            # is-view metadata is False
            self.assertFalse(stream_metadata.get('is-view'))

            # no forced-replication-method metadata
            self.assertNotIn('forced-replication-method', stream_metadata.keys())

SCENARIOS.add(MongoDBDiscovery)
