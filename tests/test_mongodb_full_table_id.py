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

def generate_simple_coll_docs(num_docs):
    docs = []
    for int_value in range(num_docs):
        docs.append({"_id": int_value, "int_field": int_value, "string_field": random_string_generator()})
    return docs

def generate_simple_binary_coll_docs(num_docs):
    docs = []
    for int_value in range(num_docs):
        docs.append({"_id": bson.Binary("test {}".format(int_value).encode()), "int_field": int_value, "string_field": random_string_generator()})
    return docs


class MongoDBFullTableID(unittest.TestCase):
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

            # simple_coll_1 has 50 documents, id is an integer instead of ObjectId
            client["simple_db"]["simple_coll_1"].insert_many(generate_simple_coll_docs(50))

            # simple_coll_2 has 100 documents, id is an integer instead of ObjectId
            client["simple_db"]["simple_coll_2"].insert_many(generate_simple_binary_coll_docs(50))

    def expected_check_streams(self):
        return {
            'simple_db-simple_coll_1',
            'simple_db-simple_coll_2'
        }

    def expected_pks(self):
        return {
            'simple_coll_1': {'_id'},
            'simple_coll_2': {'_id'}
        }

    def expected_row_counts(self):
        return {
            'simple_coll_1': 50,
            'simple_coll_2': 50
        }

    def expected_sync_streams(self):
        return {
            'simple_coll_1',
            'simple_coll_2'
        }

    def name(self):
        return "tap_tester_mongodb_full_table_id"

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

        for tap_stream_id in self.expected_check_streams():
            found_stream = [c for c in found_catalogs if c['tap_stream_id'] == tap_stream_id][0]

            # assert that the pks are correct
            self.assertEqual(self.expected_pks()[found_stream['stream_name']],
                             set(found_stream.get('metadata', {}).get('table-key-properties')))

            # assert that the row counts are correct
            self.assertEqual(self.expected_row_counts()[found_stream['stream_name']],
                             found_stream.get('metadata', {}).get('row-count'))

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
        # synthesize interrupted state
        interrupted_state = {
            'currently_syncing' : 'simple_db-simple_coll_1',
            'bookmarks' : {'simple_db-simple_coll_1': { 'max_id_value': 49,
                                                        'max_id_type': 'int',
                                                        'initial_full_table_complete': False,
                                                        'last_id_fetched': 25,
                                                        'last_id_fetched_type': 'int',
                                                        'version': int(time.time() * 1000)},
                           'simple_db-simple_coll_2': { 'max_id_value': base64.b64encode("test {}".format(49).encode()),
                                                        'max_id_type': 'bytes',
                                                        'initial_full_table_complete': False,
                                                        'last_id_fetched': base64.b64encode("test {}".format(25).encode()),
                                                        'last_id_fetched_type': 'bytes',
                                                        'version': int(time.time() * 1000)}}}

        menagerie.set_state(conn_id, interrupted_state)
        runner.run_sync_mode(self, conn_id)

        # streams that we synced are the ones that we expect to see
        records_by_stream = runner.get_records_from_target_output()
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        # ActivateVersionMessage as the last message and not the first
        for stream_name in self.expected_sync_streams():
            self.assertNotEqual('activate_version',records_by_stream[stream_name]['messages'][0]['action'])
            self.assertEqual('activate_version',records_by_stream[stream_name]['messages'][-1]['action'])

        # _id of the first record sync'd for each stream is the bookmarked
        # last_id_fetched from the interrupted_state passed to the tap
        self.assertEqual(records_by_stream['simple_coll_1']['messages'][0]['data']['_id'],
                         int(interrupted_state['bookmarks']['simple_db-simple_coll_1']['last_id_fetched']))

        # _id of the last record sync'd for each stream is the bookmarked
        # max_id_value from the interrupted_state passed to the tap
        self.assertEqual(records_by_stream['simple_coll_1']['messages'][-2]['data']['_id'],
                         int(interrupted_state['bookmarks']['simple_db-simple_coll_1']['max_id_value']))

        # assert that final state has no last_id_fetched and max_id_value bookmarks
        final_state = menagerie.get_state(conn_id)
        for tap_stream_id in self.expected_check_streams():
            self.assertIsNone(final_state['bookmarks'][tap_stream_id].get('last_id_fetched'))
            self.assertIsNone(final_state['bookmarks'][tap_stream_id].get('max_id_value'))

SCENARIOS.add(MongoDBFullTableID)
