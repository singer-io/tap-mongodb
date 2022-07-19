import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
import pymongo
import string
import random
import time
from mongodb_common import drop_all_collections, get_test_connection, ensure_environment_variables_set
import copy

RECORD_COUNT = {}


def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))


def generate_simple_coll_docs(num_docs):
    docs = []
    for int_value in range(num_docs):
        docs.append({"int_field": int_value, "string_field": random_string_generator()})
    return docs


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

    def expected_row_count_2(self):
        return {
            'simple_coll_1': 3,
            'simple_coll_2': 13
        }

    def expected_row_count_3(self):
        return {
            'simple_coll_1': 0,
            'simple_coll_2': 1
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
            interrupted_state['bookmarks']['simple_db-'+table_interrupted].update({'last_id_fetched_type': 'ObjectId'})
            interrupted_state['bookmarks']['simple_db-'+table_interrupted].update(last_id)
            interrupted_state['currently_syncing'] = 'simple_db-'+table_interrupted
            versions[tap_stream_id] = version

            # Insert Update Delete few records in the collection

            doc_to_update_1 = client["simple_db"]["simple_coll_1"].find_one()
            client["simple_db"]["simple_coll_1"].find_one_and_update({"_id": doc_to_update_1["_id"]}, {"$set": {"int_field": 999}})

            doc_to_update_2 = client["simple_db"]["simple_coll_2"].find_one()
            client["simple_db"]["simple_coll_2"].find_one_and_update({"_id": doc_to_update_2["_id"]}, {"$set": {"int_field": 888}})

            doc_to_delete_1 = client["simple_db"]["simple_coll_1"].find_one({"int_field": 2})
            client["simple_db"]["simple_coll_1"].delete_one({"_id": doc_to_delete_1["_id"]})

            doc_to_delete_2 = client["simple_db"]["simple_coll_2"].find_one({"int_field": 2})
            client["simple_db"]["simple_coll_2"].delete_one({"_id": doc_to_delete_2["_id"]})

            last_inserted_coll_1 = client["simple_db"]["simple_coll_1"].insert_many(generate_simple_coll_docs(1))
            # get the last inserted id in coll 1
            last_inserted_id_coll_1 = str(last_inserted_coll_1.inserted_ids[0])

            last_inserted_coll_2 = client["simple_db"]["simple_coll_2"].insert_many(generate_simple_coll_docs(1))
            # get the last inserted id in coll 2
            last_inserted_id_coll_2 = str(last_inserted_coll_2.inserted_ids[0])

        menagerie.set_state(conn_id, interrupted_state)

        ### Run 2nd sync ###

        second_sync = runner.run_sync_mode(self, conn_id)
        second_sync_exit_status = menagerie.get_exit_status(conn_id, second_sync)
        menagerie.verify_sync_exit_status(self, second_sync_exit_status, second_sync)

        records_by_stream_2 = runner.get_records_from_target_output()

        record_count_by_stream_2 = runner.examine_target_output_file(self,
                                                                     conn_id,
                                                                     self.expected_sync_streams(),
                                                                     self.expected_pks())

        # Verify the record count for the 2nd sync
        for tap_stream_id in self.expected_sync_streams():
            self.assertGreaterEqual(record_count_by_stream_2[tap_stream_id], self.expected_row_count_2()[tap_stream_id])

        second_state = menagerie.get_state(conn_id)

        # Verify the interrupted table is synced first
        second_sync_records = list(records_by_stream_2)
        self.assertEqual(second_sync_records[0], table_interrupted)

        # _id of the first record sync'd for each stream is the bookmarked
        # last_id_fetched from the interrupted_state passed to the tap
        self.assertEqual(records_by_stream_2['simple_coll_2']['messages'][0]['data']['_id'],
                         interrupted_state['bookmarks']['simple_db-simple_coll_2']['last_id_fetched'])

        # _id of the last record sync'd for each stream is the bookmarked
        # max_id_value from the interrupted_state passed to the tap
        self.assertEqual(records_by_stream['simple_coll_2']['messages'][-1]['data']['_id'],
                         interrupted_state['bookmarks']['simple_db-simple_coll_2']['max_id_value'])

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

            # for the previously synced tables verify the oplog_ts_time moved forward
            self.assertGreater(second_state['bookmarks'][tap_stream_id]['oplog_ts_time'],
                               initial_state['bookmarks'][tap_stream_id]['oplog_ts_time'])

        # verify the currently syncing state is none
        self.assertIsNone(second_state['currently_syncing'])

        #### Run 3rd Sync #####

        third_sync = runner.run_sync_mode(self, conn_id)
        third_sync_exit_status = menagerie.get_exit_status(conn_id, third_sync)
        menagerie.verify_sync_exit_status(self, third_sync_exit_status, third_sync)

        records_by_stream_3 = runner.get_records_from_target_output()

        record_count_by_stream_3 = runner.examine_target_output_file(self,
                                                                     conn_id,
                                                                     self.expected_sync_streams(),
                                                                     self.expected_pks())


        # Verify the record count for the 3rd sync
        for tap_stream_id in self.expected_sync_streams():
            self.assertEqual(record_count_by_stream_3[tap_stream_id], self.expected_row_count_3()[tap_stream_id])

        # verify the only record replicated is the last inserted record in the collection 2
        self.assertEqual(len(records_by_stream_3['simple_coll_2']['messages']), 2)
        self.assertEqual(records_by_stream_3['simple_coll_2']['messages'][0]['action'], 'activate_version')
        self.assertEqual(records_by_stream_3['simple_coll_2']['messages'][1]['action'], 'upsert')
        self.assertEqual(records_by_stream_3['simple_coll_2']['messages'][1]['data']['_id'], last_inserted_id_coll_2)

        third_state = menagerie.get_state(conn_id)

        # verify the third sync bookmarks matches our expectatinos
        for tap_stream_id in third_state['bookmarks'].keys():

            # verify the bookmark keys have not changed in a resumed sync
            self.assertSetEqual(
                set(third_state['bookmarks'][tap_stream_id].keys()), set(second_state['bookmarks'][tap_stream_id].keys())
            )

            # check the table versions is the same
            self.assertEqual(second_state['bookmarks'][tap_stream_id]['version'], third_state['bookmarks'][tap_stream_id]['version'])

            # verify the method has not changed
            self.assertEqual(third_state['bookmarks'][tap_stream_id]['last_replication_method'],
                             second_state['bookmarks'][tap_stream_id]['last_replication_method'])

            # verify the oplog_ts_time is the same, since there was no change in the collection
            self.assertEqual(second_state['bookmarks'][tap_stream_id]['oplog_ts_time'],
                             third_state['bookmarks'][tap_stream_id]['oplog_ts_time'])

        # verify the currently syncing state is none
        self.assertIsNone(second_state['currently_syncing'])
