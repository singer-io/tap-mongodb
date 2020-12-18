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
        docs.append({"int_field": int_value, "string_field": random_string_generator()})
    return docs

class MongoDBFullTableInterruptible(unittest.TestCase):
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

    def expected_check_streams(self):
        return {
            'simple_db-simple_coll_1',
            'simple_db-simple_coll_2',
        }

    def expected_pks(self):
        return {
            'simple_coll_1': {'_id'},
            'simple_coll_2': {'_id'},
        }

    def expected_row_counts(self):
        return {
            'simple_coll_1': 25,
            'simple_coll_2': 50,
        }

    def expected_sync_streams(self):
        return {
            'simple_coll_1',
            'simple_coll_2'
        }

    def name(self):
        return "tap_tester_mongodb_full_table_interruptible"

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

        # verify check exit codes
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
        # Select simple_coll_1 and simple_coll_2 streams and add replication method metadata
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'FULL_TABLE'}}]
            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                    stream_catalog,
                                                                                    annotated_schema,
                                                                                    additional_md)
        # Synthesize interrupted state
        interrupted_state = {
            'currently_syncing' : 'simple_db-simple_coll_1',
            'bookmarks' : {}
        }

        versions = {}
        with get_test_connection() as client:
            for stream_name in self.expected_sync_streams():
                rows = [x for x in client['simple_db'][stream_name].find(sort=[("_id", pymongo.ASCENDING)])]
                # set last_id_fetched to middle point of table
                last_id_fetched = str(rows[int(len(rows)/2)]['_id'])
                max_id_value = str(rows[-1]['_id'])

                tap_stream_id = 'simple_db-'+stream_name
                version = int(time.time() * 1000)
                interrupted_state['bookmarks'][tap_stream_id] = {
                    'max_id_value': max_id_value,
                    'max_id_type': 'ObjectId',
                    'initial_full_table_complete': False,
                    'last_id_fetched': last_id_fetched,
                    'last_id_fetched_type': 'ObjectId',
                    'version': version
                }
                versions[tap_stream_id] = version

        menagerie.set_state(conn_id, interrupted_state)

        runner.run_sync_mode(self, conn_id)

        # streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        # record counts
        records_by_stream = runner.get_records_from_target_output()
        self.assertEqual(self.expected_row_counts(), record_count_by_stream)

        # ActivateVersionMessage as the last message and not the first
        for stream_name in self.expected_sync_streams():
            self.assertNotEqual('activate_version',records_by_stream[stream_name]['messages'][0]['action'])
            self.assertEqual('activate_version',records_by_stream[stream_name]['messages'][-1]['action'])

        # _id of the first record sync'd for each stream is the bookmarked
        # last_id_fetched from the interrupted_state passed to the tap
        self.assertEqual(records_by_stream['simple_coll_1']['messages'][0]['data']['_id'],
                         interrupted_state['bookmarks']['simple_db-simple_coll_1']['last_id_fetched'])
        self.assertEqual(records_by_stream['simple_coll_2']['messages'][0]['data']['_id'],
                         interrupted_state['bookmarks']['simple_db-simple_coll_2']['last_id_fetched'])

        # _id of the last record sync'd for each stream is the bookmarked
        # max_id_value from the interrupted_state passed to the tap
        self.assertEqual(records_by_stream['simple_coll_1']['messages'][-2]['data']['_id'],
                         interrupted_state['bookmarks']['simple_db-simple_coll_1']['max_id_value'])
        self.assertEqual(records_by_stream['simple_coll_2']['messages'][-2]['data']['_id'],
                         interrupted_state['bookmarks']['simple_db-simple_coll_2']['max_id_value'])

        # assert that final state has no last_id_fetched and max_id_value bookmarks
        final_state = menagerie.get_state(conn_id)
        for tap_stream_id in self.expected_check_streams():
            self.assertIsNone(final_state['bookmarks'][tap_stream_id].get('last_id_fetched'))
            self.assertIsNone(final_state['bookmarks'][tap_stream_id].get('max_id_value'))

        state = menagerie.get_state(conn_id)
        for tap_stream_id, stream_bookmarks in state.get('bookmarks', {}).items():
            self.assertTrue(stream_bookmarks.get('initial_full_table_complete', False))

SCENARIOS.add(MongoDBFullTableInterruptible)
