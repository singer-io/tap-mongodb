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
from mongodb_common import drop_all_collections, get_test_connection, ensure_environment_variables_set
import decimal
import copy


RECORD_COUNT = {}


def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


def generate_simple_coll_docs(num_docs):
    docs = []
    for int_value in range(num_docs):
        docs.append({"int_field": int_value, "string_field": random_string_generator()})
    return docs

table_synced = "simple_coll_1"
table_interrupted = "simple_coll_2"


class MongoDBLogBasedInterruptible(unittest.TestCase):
    def setUp(self):
        ensure_environment_variables_set()

        with get_test_connection() as client:
            # drop all dbs/collections
            drop_all_collections(client)

            # simple_coll_1 has 10 documents
            client["simple_db"]["simple_coll_1"].insert_many(generate_simple_coll_docs(10))

            # simple_coll_2 has 20 documents
            client["simple_db"]["simple_coll_2"].insert_many(generate_simple_coll_docs(20))

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

    def expected_row_count_1(self):
        return {
            'simple_coll_1': 10,
            'simple_coll_2': 20,
        }

    def expected_sync_streams(self):
        return {
            'simple_coll_1',
            'simple_coll_2'
        }

    def name(self):
        return "tap_tester_mongodb_log_based_interruptible"

    def tap_name(self):
        return "tap-mongodb"

    def get_type(self):
        return "platform.mongodb"

    def get_credentials(self):
        return {'password': os.getenv('TAP_MONGODB_PASSWORD')}

    def get_properties(self):
        return {'host': os.getenv('TAP_MONGODB_HOST'),
                'port': os.getenv('TAP_MONGODB_PORT'),
                'user': os.getenv('TAP_MONGODB_USER'),
                'database': os.getenv('TAP_MONGODB_DBNAME')
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

        for tap_stream_id in self.expected_check_streams():
            found_stream = [c for c in found_catalogs if c['tap_stream_id'] == tap_stream_id][0]

            # assert that the pks are correct
            self.assertEqual(self.expected_pks()[found_stream['stream_name']],
                             set(found_stream.get('metadata', {}).get('table-key-properties')))

            # assert that the row counts are correct
            self.assertEqual(self.expected_row_count_1()[found_stream['stream_name']],
                             found_stream.get('metadata', {}).get('row-count'))


        #  -----------------------------------
        # -----------Initial Full Table Sync ---------
        #  -----------------------------------
        # Select simple_coll_1 and simple_coll_2 streams and add replication method metadata
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [{"breadcrumb": [], "metadata": {'replication-method': 'LOG_BASED'}}]
            selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                   stream_catalog,
                                                                                   annotated_schema,
                                                                                   additional_md)

        runner.run_sync_mode(self, conn_id)

        # streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        records_by_stream = runner.get_records_from_target_output()

        for tap_stream_id in self.expected_sync_streams():
            self.assertGreaterEqual(record_count_by_stream[tap_stream_id], self.expected_row_count_1()[tap_stream_id])

        initial_state = menagerie.get_state(conn_id)
        bookmarks = initial_state['bookmarks']

        self.assertIsNone(initial_state['currently_syncing'])

        # verify each bookmark matches our expectation prior to setting the interrupted state
        for table_name in self.expected_sync_streams():

            table_bookmark = bookmarks['simple_db-'+table_name]
            bookmark_keys = set(table_bookmark.keys())

            self.assertIn('version', bookmark_keys)
            self.assertIn('last_replication_method', bookmark_keys)
            self.assertIn('initial_full_table_complete', bookmark_keys)

            self.assertIn('oplog_ts_time', bookmark_keys)
            self.assertIn('oplog_ts_inc', bookmark_keys)
            self.assertNotIn('replication_key', bookmark_keys)

            self.assertEqual('LOG_BASED', table_bookmark['last_replication_method'])
            self.assertTrue(table_bookmark['initial_full_table_complete'])
            self.assertIsInstance(table_bookmark['version'], int)

        # Synthesize interrupted state
        interrupted_state = copy.deepcopy(initial_state)

        versions = {}
        with get_test_connection() as client:
            rows = [x for x in client['simple_db'][table_interrupted].find(sort=[("_id", pymongo.ASCENDING)])]
            # set last_id_fetched to middle point of table
            last_id_fetched = str(rows[int(len(rows)/2)]['_id'])
            last_id = {'last_id_fetched': last_id_fetched}
            max_id_value = str(rows[-1]['_id'])
            max_id = {'max_id_value': max_id_value}
            del interrupted_state['bookmarks']['simple_db-'+table_interrupted]['initial_full_table_complete']
            version = int(time.time() * 1000)
            # add the id bookmarks
            interrupted_state['bookmarks']['simple_db-'+table_interrupted].update(max_id)
            interrupted_state['bookmarks']['simple_db-'+table_interrupted].update({'max_id_type': 'ObjectId'})
            interrupted_state['bookmarks']['simple_db-'+table_interrupted].update({'last_id_type': 'ObjectId'})
            interrupted_state['bookmarks']['simple_db-'+table_interrupted].update(last_id)
            interrupted_state['currently_syncing'] = 'simple_db-'+table_interrupted
            versions[tap_stream_id] = version

        # update existing documents in collection with int_field value less than 25, and verify they do not come up in the sync
        # update existing documents in collection with int_field value greater than 25, and verify they come up in the sync

            # find_one() is going to retreive the first document in the collection
            doc_to_update_1 = client["simple_db"]["simple_coll_1"].find_one()
            client["simple_db"]["simple_coll_1"].find_one_and_update({"_id": doc_to_update_1["_id"]}, {"$set": {"int_field": 999}})

            doc_to_update_2 = client["simple_db"]["simple_coll_2"].find_one()
            client["simple_db"]["simple_coll_2"].find_one_and_update({"_id": doc_to_update_2["_id"]}, {"$set": {"int_field": 888}})

            doc_to_update_3 = client["simple_db"]["simple_coll_1"].find_one({"int_field": 9})
            client["simple_db"]["simple_coll_1"].find_one_and_update({"_id": doc_to_update_3["_id"]}, {"$set": {"int_field": 777}})

            doc_to_update_4 = client["simple_db"]["simple_coll_2"].find_one({"int_field": 19})
            client["simple_db"]["simple_coll_2"].find_one_and_update({"_id": doc_to_update_4["_id"]}, {"$set": {"int_field": 666}})
            client["simple_db"]["simple_coll_1"].insert_many(generate_simple_coll_docs(5))

            client["simple_db"]["simple_coll_2"].insert_many(generate_simple_coll_docs(5))

        menagerie.set_state(conn_id, interrupted_state)

        ### Run 2nd sync ###

        import ipdb; ipdb.set_trace()
        1+1
        second_sync = runner.run_sync_mode(self, conn_id)
        second_sync_exit_status = menagerie.get_exit_status(conn_id, second_sync)
        menagerie.verify_sync_exit_status(self, second_sync_exit_status, second_sync)

        records_by_stream_2 = runner.get_records_from_target_output()

        record_count_by_stream_2 = runner.examine_target_output_file(self,
                                                                     conn_id,
                                                                     self.expected_sync_streams(),
                                                                     self.expected_pks())
        second_state = menagerie.get_state(conn_id)

        # Verify the interrupted table is synced first
        self.assertEqual(records_by_stream_2[0]['table_name'], table_interrupted)

        # verify the second sync bookmarks matches our expectatinos
        for tap_stream_id in initial_state['bookmarks'].keys():

            # verify the bookmark keys have not changed in a resumed sync
            self.assertSetEqual(
                set(initial_state['bookmarks'][tap_stream_id].keys()), set(second_state['bookmarks'][tap_stream_id].keys())
            )

            # check the table versions is the same
            self.assertEqual(second_state['bookmarks'][tap_stream_id]['version'], initial_state['bookmarks'][tap_stream_id]['version'])

            # verify the method has not changed
            self.assertEqual(initial_state['bookmarks'][tap_stream_id]['last_replication_method'],
                             second_state['bookmarks'][tap_stream_id]['last_replication_method'])

            # verify the resumed sync has completed
            self.assertTrue(second_state['bookmarks'][tap_stream_id]['initial_full_table_complete'])

            # for the previously synced tables verify the oplog_ts_inc moved forward
            if tap_stream_id != f'{self.test_db_name}-{table_interrupted}':
                self.assertGreater(second_state['bookmarks'][tap_stream_id]['oplog_ts_inc'],
                                   initial_state['bookmarks'][tap_stream_id]['oplog_ts_inc'])
            else:
                self.assertEqual(second_state['bookmarks'][tap_stream_id]['oplog_ts_inc'],
                                 initial_state['bookmarks'][tap_stream_id]['oplog_ts_inc'])

        # verify the currently syncing state is none
        self.assertIsNone(second_state['currently_syncing'])

        # # verify number records/messages for interrupted table
        # messages = ordered_messages_by_stream[table_interrupted]
        # self.assertEqual(1, record_count_by_stream_2[table_interrupted])
        # self.assertEqual(2, len(messages))

        # # verify values of records/messages for the interrupted table
        # self.assertDictEqual(self.expected_records[table_interrupted][3], messages[0]['rec'])
        # self.assertEqual('switch_view', messages[1]['action'])

        