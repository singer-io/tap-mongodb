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
from bson import ObjectId
import uuid
import singer
from functools import reduce
from singer import utils, metadata
import decimal
from mongodb_common import drop_all_collections
from datetime import datetime, timedelta


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

def z_string_generator(size=6):
    return 'z' * size

def generate_simple_coll_docs(num_docs):
    docs = []
    start_datetime = datetime(2018, 1, 1, 19, 29, 14, 578000)

    for int_value in range(num_docs):
        start_datetime = start_datetime + timedelta(days=5)
        docs.append({"int_field": int_value,
                     "string_field": z_string_generator(int_value),
                     "date_field": start_datetime,
                     "double_field": int_value+1.00001,
                     "timestamp_field": bson.timestamp.Timestamp(int_value+1565897157, 1),
                     "uuid_field": uuid.UUID('3e139ff5-d622-45c6-bf9e-1dfec7282{:03d}'.format(int_value)),
                     "64_bit_int_field": 34359738368 + int_value
                     })
    return docs

class MongoDBIncremental(unittest.TestCase):
    def key_names(self):
        return ['int_field',
                'string_field',
                'date_field',
                'timestamp_field',
                'uuid_field',
                '64_bit_int_field',
                'double_field']

    def setUp(self):
        if not all([x for x in [os.getenv('TAP_MONGODB_HOST'),
                                    os.getenv('TAP_MONGODB_USER'),
                                    os.getenv('TAP_MONGODB_PASSWORD'),
                                    os.getenv('TAP_MONGODB_PORT'),
                                    os.getenv('TAP_MONGODB_DBNAME')]]):

            #pylint: disable=line-too-long
            raise Exception("set TAP_MONGODB_HOST, TAP_MONGODB_USER, TAP_MONGODB_PASSWORD, TAP_MONGODB_PORT, TAP_MONGODB_DBNAME")


        with get_test_connection() as client:
            ############# Drop all dbs/collections #############
            drop_all_collections(client)

            ############# Add simple collections #############
            # simple_coll_1 has 50 documents
            client["simple_db"]["simple_coll_1"].insert_many(generate_simple_coll_docs(50))

            # simple_coll_2 has 100 documents
            client["simple_db"]["simple_coll_2"].insert_many(generate_simple_coll_docs(100))

            ############# Add Index on date_field ############
            client["simple_db"]["simple_coll_1"].create_index([("date_field", pymongo.ASCENDING)])
            client["simple_db"]["simple_coll_2"].create_index([("date_field", pymongo.ASCENDING)])

            # Add simple_coll per key type
            for key_name in self.key_names():
                client["simple_db"]["simple_coll_{}".format(key_name)].insert_many(generate_simple_coll_docs(50))

                # add index on field
                client["simple_db"]["simple_coll_{}".format(key_name)].create_index([(key_name, pymongo.ASCENDING)])


    def expected_check_streams(self):
        return {
            'simple_db-simple_coll_1',
            'simple_db-simple_coll_2',
            *['simple_db-simple_coll_{}'.format(k) for k in self.key_names()]
        }

    def expected_pks(self):
        return {
            'simple_coll_1': {'_id'},
            'simple_coll_2': {'_id'},
            **{"simple_coll_{}".format(k): {'_id'} for k in self.key_names()}
        }

    def expected_valid_replication_keys(self):
        return {
            'simple_coll_1': {'_id', 'date_field'},
            'simple_coll_2': {'_id', 'date_field'},
            **{"simple_coll_{}".format(k): {'_id', k} for k in self.key_names()}
        }

    def expected_row_counts(self):
        return {
            'simple_coll_1': 50,
            'simple_coll_2': 100,
            **{"simple_coll_{}".format(k): 50 for k in self.key_names()}
        }

    def expected_last_sync_row_counts(self):
        return {
            'simple_coll_1': 50,
            'simple_coll_2': 100,
            **{"simple_coll_{}".format(k): 1 for k in self.key_names()}
        }

    def expected_incremental_int_fields(self):
        return {
            'simple_coll_1': {49, 50, 51},
            'simple_coll_2': {99, 100, 101},
            **{"simple_coll_{}".format(k): {49, 50, 51} for k in self.key_names()}
        }

    def expected_sync_streams(self):
        return {
            'simple_coll_1',
            'simple_coll_2',
            *['simple_coll_{}'.format(k) for k in self.key_names()]
        }

    def name(self):
        return "tap_tester_mongodb_incremental"

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
        catalog = menagerie.get_catalog(conn_id)
        found_catalogs = menagerie.get_catalogs(conn_id)

        # assert we find the correct streams
        self.assertEqual(self.expected_check_streams(),
                         {c['tap_stream_id'] for c in catalog['streams']})


        for tap_stream_id in self.expected_check_streams():
            found_stream = [c for c in catalog['streams'] if c['tap_stream_id'] == tap_stream_id][0]
            stream_metadata = [x['metadata'] for x in found_stream['metadata'] if x['breadcrumb']==[]][0]

            # assert that the pks are correct
            self.assertEqual(self.expected_pks()[found_stream['stream']],
                             set(stream_metadata.get('table-key-properties')))

            # assert that the row counts are correct
            self.assertEqual(self.expected_row_counts()[found_stream['stream']],
                             stream_metadata.get('row-count'))

            # assert that valid replication keys are correct
            self.assertEqual(self.expected_valid_replication_keys()[found_stream['stream']],
                             set(stream_metadata.get('valid-replication-keys')))

        #  -----------------------------------
        # ----------- Initial Sync ---------
        #  -----------------------------------
        # Select simple_coll_1 and simple_coll_2 streams and add replication method metadata
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            rep_key = 'date_field'
            for key in self.key_names():
                if key in stream_catalog['stream_name']:
                    rep_key = key
            additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'INCREMENTAL',
                                                                'replication-key': rep_key}}]
            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                   stream_catalog,
                                                                                   annotated_schema,
                                                                                   additional_md)

        # Run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)


        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()


        # assert that each of the streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())


        # Verify that the full table was syncd
        for tap_stream_id in self.expected_sync_streams():
            self.assertEqual(self.expected_row_counts()[tap_stream_id],
                             record_count_by_stream[tap_stream_id])

        # Verify that we have 'initial_full_table_complete' bookmark
        state = menagerie.get_state(conn_id)
        first_versions = {}

        # -----------------------------------
        # ------------ Second Sync ----------
        # -----------------------------------

        # Add some records
        with get_test_connection() as client:

            # insert two documents with date_field > bookmark for next sync
            client["simple_db"]["simple_coll_1"].insert_one({
                "int_field": 50,
                "string_field": z_string_generator(),
                "date_field": datetime(2018, 9, 13, 19, 29, 14, 578000),
                "double_field": 51.001,
                "timestamp_field": bson.timestamp.Timestamp(1565897157+50,1),
                "uuid_field": uuid.UUID('3e139ff5-d622-45c6-bf9e-1dfec7282050'),
                "64_bit_int_field": 34359738368 + 50
            })
            client["simple_db"]["simple_coll_1"].insert_one({
                "int_field": 51,
                "string_field": z_string_generator(),
                "date_field": datetime(2018, 9, 18, 19, 29, 14, 578000),
                "double_field": 52.001,
                "timestamp_field": bson.timestamp.Timestamp(1565897157+51,1),
                "uuid_field": uuid.UUID('3e139ff5-d622-45c6-bf9e-1dfec7282051'),
                "64_bit_int_field": 34359738368 + 51
            })

            client["simple_db"]["simple_coll_2"].insert_one({
                "int_field": 100,
                "string_field": z_string_generator(),
                "date_field": datetime(2019, 5, 21, 19, 29, 14, 578000),
                "double_field": 101.001,
                "timestamp_field": bson.timestamp.Timestamp(1565897157+100,1),
                "uuid_field": uuid.UUID('3e139ff5-d622-45c6-bf9e-1dfec7282100'),
                "64_bit_int_field": 34359738368 + 100
            })
            client["simple_db"]["simple_coll_2"].insert_one({
                "int_field": 101,
                "string_field": z_string_generator(),
                "date_field": datetime(2019, 5, 26, 19, 29, 14, 578000),
                "double_field": 102.001,
                "timestamp_field": bson.timestamp.Timestamp(1565897157+101,1),
                "uuid_field": uuid.UUID('3e139ff5-d622-45c6-bf9e-1dfec7282101'),
                "64_bit_int_field": 34359738368 + 101
            })

            for key_name in self.key_names():
                client["simple_db"]["simple_coll_{}".format(key_name)].insert_one({
                    "int_field": 50,
                    "string_field": z_string_generator(50),
                    "date_field": datetime(2018, 9, 13, 19, 29, 15, 578000),
                    "double_field": 51.001,
                    "timestamp_field": bson.timestamp.Timestamp(1565897157+50,1),
                    "uuid_field": uuid.UUID('3e139ff5-d622-45c6-bf9e-1dfec7282050'),
                    "64_bit_int_field": 34359738368 + 50
                })
                client["simple_db"]["simple_coll_{}".format(key_name)].insert_one({
                    "int_field": 51,
                    "string_field": z_string_generator(51),
                    "date_field": datetime(2018, 9, 18, 19, 29, 16, 578000),
                    "double_field": 52.001,
                    "timestamp_field": bson.timestamp.Timestamp(1565897157+51,1),
                    "uuid_field": uuid.UUID('3e139ff5-d622-45c6-bf9e-1dfec7282051'),
                    "64_bit_int_field": 34359738368 + 51
                })


        # Run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()
        records_by_stream = {}
        for stream_name in self.expected_sync_streams():
            records_by_stream[stream_name] = [x for x in messages_by_stream[stream_name]['messages'] if x.get('action') == 'upsert']

        # assert that each of the streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        # Verify that we got 3 records for each stream (2 because of the new records, 1 because
        # of gte)
        for k,v in record_count_by_stream.items():
            self.assertEqual(3, v)

        # Verify that the _id of the records sent are the same set as the
        # _ids of the documents changed
        for stream_name in self.expected_sync_streams():
            actual = set([x['data']['int_field'] for x in records_by_stream[stream_name]])
            self.assertEqual(self.expected_incremental_int_fields()[stream_name], actual)

        # -----------------------------------
        # ------------ Third Sync -----------
        # -----------------------------------
        # Change the replication method for simple_coll_1
        # Change the replication key for simple_coll_2
        # Make sure both do full resync
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = []
            if stream_catalog['tap_stream_id'] == 'simple_db-simple_coll_1':
                additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'LOG_BASED'}}]
            elif stream_catalog['tap_stream_id'] == 'simple_db-simple_coll_2':
                additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'INCREMENTAL',
                                                                    'replication-key': 'timestamp_field'}}]
            else:
                additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'INCREMENTAL',
                                                                    'replication-key': stream_catalog['stream_name'].replace('simple_coll_', '')}}]

            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                   stream_catalog,
                                                                                   annotated_schema,
                                                                                   additional_md)
        # Run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())
        for tap_stream_id in self.expected_sync_streams():
            self.assertGreaterEqual(record_count_by_stream[tap_stream_id],self.expected_last_sync_row_counts()[tap_stream_id])



SCENARIOS.add(MongoDBIncremental)
