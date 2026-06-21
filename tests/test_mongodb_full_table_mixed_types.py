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
import singer
import uuid
import base64
from functools import reduce
from singer import utils, metadata
from mongodb_common import drop_all_collections
import decimal


RECORD_COUNT = {}

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

def generate_simple_coll_docs_obj_id(num_docs):
    return [{"_id": bson.objectid.ObjectId(), "int_field": i, "string_field": random_string_generator()}
            for i in range(num_docs)]


def generate_simple_coll_docs_uuid(num_docs):
    return [{"_id": str(uuid.uuid4()), "int_field": i, "string_field": random_string_generator()}
            for i in range(num_docs)]


class MongoDBFullTableMixedTypes(unittest.TestCase):
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

            # simple_coll_1 has 100 documents, 20 of them have id is an ObjectId
            # and 80 of them use a UUID
            client["simple_db"]["simple_coll_1"].insert_many(generate_simple_coll_docs_obj_id(20))
            client["simple_db"]["simple_coll_1"].insert_many(generate_simple_coll_docs_uuid(80))


    def expected_check_streams(self):
        return {
            'simple_db-simple_coll_1',
        }

    def expected_pks(self):
        return {
            'simple_coll_1': {'_id'},
        }

    def expected_row_counts(self):
        return {
            'simple_coll_1': 100
        }

    def expected_sync_streams(self):
        return {
            'simple_coll_1'
        }

    def name(self):
        return "tap_tester_mongodb_full_table_mixed_types"

    def tap_name(self):
        return "tap_tester_mongodb_full_table_mixed_types"

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

        #  -------------------------------
        # -----------  Discovery ----------
        #  -------------------------------

        # run in discovery mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # verify the tap discovered the right streams
        found_catalogs = menagerie.get_catalogs(conn_id)

        # assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         {c['tap_stream_id'] for c in found_catalogs})

        #  -----------------------------------
        # ----------- Full Table Sync ---------
        #  -----------------------------------
        # select simple_coll_1 stream and add replication method metadata
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'FULL_TABLE'}}]
            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                    stream_catalog,
                                                                                    annotated_schema,
                                                                                    additional_md)
        # Run Sync Mode
        runner.run_sync_mode(self, conn_id)

        # streams that we synced are the ones that we expect to see
        records_by_stream = runner.get_records_from_target_output()
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        # There should be 100 records, looking at upsert messages
        self.assertEqual(100, len([r for r in records_by_stream['simple_coll_1']['messages'] if r['action'] == 'upsert']))

SCENARIOS.add(MongoDBFullTableMixedTypes)
