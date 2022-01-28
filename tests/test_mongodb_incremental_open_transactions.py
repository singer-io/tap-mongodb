import os
import uuid
import decimal
import string
import bson
from datetime import datetime, timedelta
import unittest
from unittest import TestCase
import pymongo
import random
from tap_tester import connections, menagerie, runner
from mongodb_common import drop_all_collections, get_test_connection, ensure_environment_variables_set

RECORD_COUNT = {}


def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def generate_simple_coll_docs(num_docs):
    docs = []
    for int_value in range(num_docs):
        docs.append({"int_field": int_value, "string_field": random_string_generator()})
    return docs

class MongoDBOpenTransactions(unittest.TestCase):

    def expected_check_streams_sync_1(self):
        return {
            'simple_db-simple_coll_1',
            'simple_db-simple_coll_2',
            'simple_db-simple_coll_3'
        }

    def expected_check_streams_sync_2(self):
        return {
            'simple_db-simple_coll_1',
            'simple_db-simple_coll_2',
            'simple_db-simple_coll_3'
        }

    def expected_pks_1(self):
        return {
            'simple_db_simple_coll_1': {'_id'},
            'simple_db_simple_coll_2': {'_id'},
            'simple_db_simple_coll_3': {'_id'}
        }

    def expected_pks_2(self):
        return {
            'simple_db_simple_coll_1': {'_id'},
            'simple_db_simple_coll_2': {'_id'},
            'simple_db_simple_coll_3': {'_id'}
        }

    def expected_row_counts_sync_1(self):
        return {
            'simple_db_simple_coll_1': 10,
            'simple_db_simple_coll_2': 20,
            'simple_db_simple_coll_3': 0
        }

    def expected_row_counts_sync_2(self):
        return {
            'simple_db_simple_coll_1': 2,
            'simple_db_simple_coll_2': 2,
            'simple_db_simple_coll_3': 5
        }

    def expected_row_counts_sync_3(self):
        return {
            'simple_db_simple_coll_1': 0,
            'simple_db_simple_coll_2': 0,
            'simple_db_simple_coll_3': 0
        }

    def expected_sync_streams_1(self):
        return {
            'simple_db_simple_coll_1',
            'simple_db_simple_coll_2',
            'simple_db_simple_coll_3'
        }

    def expected_sync_streams_2(self):
        return {
            'simple_db_simple_coll_1',
            'simple_db_simple_coll_2',
            'simple_db_simple_coll_3'
        }

    def name(self):
        return "tap_tester_mongodb_open_transaction"

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
                'database' : os.getenv('TAP_MONGODB_DBNAME'),
                'include_schemas_in_destination_stream_name': 'true'
        }

    def test_run(self):

        ensure_environment_variables_set()

        with get_test_connection() as client:
            # drop all dbs/collections
            drop_all_collections(client)



            #################
            # Session 1
            #################

            session1 = client.start_session()

            session1.start_transaction()

            # simple_coll_1 has 10 documents
            client["simple_db"]["simple_coll_1"].insert_many(generate_simple_coll_docs(10))

            # simple_coll_2 has 20 documents
            client["simple_db"]["simple_coll_2"].insert_many(generate_simple_coll_docs(20))

            session1.commit_transaction()

            ################
            # Session 2
            ################

            session2 = client.start_session()

            session2.start_transaction()

            # simple_coll_3 is an empty collection
            client["simple_db"].create_collection("simple_coll_3")

            # update document from coll 1 and coll 2
            client["simple_db"]["simple_coll_1"].update_one({"int_field": 5}, {"$set": {"int_field": 11}}, session=session2)
            client["simple_db"]["simple_coll_2"].update_one({"int_field": 10}, {"$set": {"int_field": 21}}, session=session2)

            # insert document to coll 3
            client["simple_db"]["simple_coll_3"].insert_many(generate_simple_coll_docs(5), session=session2)

            # deletes do not matter in incremental replication, invalid scenario to test

            conn_id = connections.ensure_connection(self)

            # run in discovery mode
            check_job_name = runner.run_check_mode(self, conn_id)

            # verify check exit codes
            exit_status = menagerie.get_exit_status(conn_id, check_job_name)
            menagerie.verify_check_exit_status(self, exit_status, check_job_name)

            # verify the tap discovered the right streams
            found_catalogs = menagerie.get_catalogs(conn_id)

            # assert we find the correct streams which includes all collections which are part of session1 and session2
            self.assertEqual(self.expected_check_streams_sync_1(),
                             {c['tap_stream_id'] for c in found_catalogs})

            # Select streams and add replication method metadata
            for stream_catalog in found_catalogs:
                annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
                additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'INCREMENTAL', 'replication_key': 'int_field'}}]
                selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                        stream_catalog,
                                                                                        annotated_schema,
                                                                                        additional_md)

            # run full table sync
            sync_job_name = runner.run_sync_mode(self, conn_id)

            # check exit status
            exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
            menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

            # streams that we synced are the ones that we expect to see
            records_by_stream = runner.get_records_from_target_output()
            record_count_by_stream = runner.examine_target_output_file(self,
                                                                       conn_id,
                                                                       self.expected_sync_streams_1(),
                                                                       self.expected_pks_1())

            # validate the record count in collections which are part of session1 and session2, should not read updates on coll 1 and coll 2 and insert on coll 3. Because the transaction is not committed
            self.assertEqual(self.expected_row_counts_sync_1(), record_count_by_stream)

            session2.commit_transaction()

            ################
            # Session 3
            ################

            session3 = client.start_session()

            session3.start_transaction()

            # Run 2nd sync
            # run in discovery mode

            sync_2 = runner.run_sync_mode(self, conn_id)
            exit_status_2 = menagerie.get_exit_status(conn_id, sync_2)
            menagerie.verify_sync_exit_status(self, exit_status_2, sync_2)

            records_by_stream_2 = runner.get_records_from_target_output()
            record_count_by_stream_2 = runner.examine_target_output_file(self,
                                                                         conn_id,
                                                                         self.expected_sync_streams_2(),
                                                                         self.expected_pks_2())
            # validate that we see the updates to coll 1 and coll 2 and insert to coll 3 in the 2nd sync
            # we see 2 records for coll 1 and coll 2, 1 record for update and the other record for the bookmarked record
            self.assertEqual(self.expected_row_counts_sync_2(), record_count_by_stream_2)

            # Test case to validate tap behaviour when we delete bookmarked document and run sync
            state_2 = menagerie.get_state(conn_id)

            for stream in self.expected_check_streams_sync_1():
                rep_key_value = state_2['bookmarks'][stream]['replication_key_value']
                if stream == 'simple_db-simple_coll_1':
                    collection = 'simple_coll_1'
                elif stream == 'simple_db-simple_coll_2':
                    collection = 'simple_coll_2'
                elif stream == 'simple_db-simple_coll_3':
                    collection = 'simple_coll_3'
                client["simple_db"][collection].delete_one({"int_field": int(rep_key_value)}, session=session3)

            session3.commit_transaction()

            ###############
            # Session 4
            ###############

            state_3 = menagerie.get_state(conn_id)
            sync_3 = runner.run_sync_mode(self, conn_id)
            exit_status_3 = menagerie.get_exit_status(conn_id, sync_3)
            menagerie.verify_sync_exit_status(self, exit_status_3, sync_3)
            records_by_stream_3 = runner.get_records_from_target_output()
            record_count_by_stream_3 = runner.examine_target_output_file(self,
                                                                         conn_id,
                                                                         self.expected_sync_streams_2(),
                                                                         self.expected_pks_2())

            # validate that we see 0 records being replicated because we deleted the bookmark value on each of the collection
            self.assertEqual(self.expected_row_counts_sync_3(), record_count_by_stream_3)

            # validate that the state value has not changed after deleting the bookmarked value in each collection
            self.assertEqual(state_2, state_3)
