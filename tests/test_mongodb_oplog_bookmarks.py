import os
import pymongo
import random
import string
import time
import unittest

from mongodb_common import drop_all_collections, get_test_connection, ensure_environment_variables_set
from tap_tester import connections, menagerie, runner


RECORD_COUNT = {}


def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def generate_simple_coll_docs(num_docs):
    docs = []
    for int_value in range(num_docs):
        docs.append({"int_field": int_value, "string_field": random_string_generator()})
    return docs

class MongoDBOplogBookmarks(unittest.TestCase):
    def setUp(self):

        ensure_environment_variables_set()

        with get_test_connection() as client:
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
            'simple_coll_1': 50,
            'simple_coll_2': 100,

        }


    def expected_sync_streams(self):
        return {
            'simple_coll_1',
            'simple_coll_2',
        }

    def name(self):
        return "tap_tester_mongodb_oplog_bookmarks"

    def tap_name(self):
        return "tap-mongodb"

    def get_type(self):
        return "platform.mongodb"

    def get_credentials(self):
        return {'password': os.getenv('TAP_MONGODB_PASSWORD')}

    def get_properties(self):
        return {
            'host' : os.getenv('TAP_MONGODB_HOST'),
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
        # ----------- Initial Full Table ---------
        #  -----------------------------------
        # Select simple_coll_1 and add replication method metadata
        additional_md = [{ "breadcrumb" : [],
                           "metadata" : {'replication-method' : 'LOG_BASED'}}]
        for stream_catalog in found_catalogs:
            if stream_catalog['tap_stream_id'] == 'simple_db-simple_coll_1':
                annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
                selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id,
                                                                                       stream_catalog,
                                                                                       annotated_schema,
                                                                                       additional_md)

        # Run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)


        # verify the persisted schema was correct
        records_by_stream = runner.get_records_from_target_output()

        # assert that each of the streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        # Verify that the full table was synced
        tap_stream_id = 'simple_db-simple_coll_1'
        self.assertGreaterEqual(record_count_by_stream['simple_coll_1'],
                                self.expected_row_counts()['simple_coll_1'])

        # Verify that we have 'initial_full_table_complete' bookmark
        state = menagerie.get_state(conn_id)
        first_versions = {}

        # assert that the state has an initial_full_table_complete == True
        self.assertTrue(state['bookmarks'][tap_stream_id]['initial_full_table_complete'])
        # assert that there is a version bookmark in state
        first_versions[tap_stream_id] = state['bookmarks'][tap_stream_id]['version']
        self.assertIsNotNone(first_versions[tap_stream_id])
        # Verify that we have a oplog_ts_time and oplog_ts_inc bookmark
        self.assertIsNotNone(state['bookmarks'][tap_stream_id]['oplog_ts_time'])
        self.assertIsNotNone(state['bookmarks'][tap_stream_id]['oplog_ts_inc'])
        
        # Store initial bookmark values for later comparison
        initial_oplog_ts_time = state['bookmarks'][tap_stream_id]['oplog_ts_time']
        initial_oplog_ts_inc = state['bookmarks'][tap_stream_id]['oplog_ts_inc']



        # Insert records to coll_1 to get the bookmark to be a ts on coll_1
        with get_test_connection() as client:
            client["simple_db"]["simple_coll_1"].insert_one({"int_field": 101, "string_field": random_string_generator()})
        sync_job_name = runner.run_sync_mode(self, conn_id)


        changed_ids = set()
        with get_test_connection() as client:
            # Make changes to not selected collection
            changed_ids.add(client['simple_db']['simple_coll_2'].find({'int_field': 0})[0]['_id'])
            client["simple_db"]["simple_coll_2"].delete_one({'int_field': 0})

            changed_ids.add(client['simple_db']['simple_coll_2'].find({'int_field': 1})[0]['_id'])
            client["simple_db"]["simple_coll_2"].delete_one({'int_field': 1})

            changed_ids.add(client['simple_db']['simple_coll_2'].find({'int_field': 98})[0]['_id'])
            client["simple_db"]["simple_coll_2"].update_one({'int_field': 98},{'$set': {'int_field': -1}})

            changed_ids.add(client['simple_db']['simple_coll_2'].find({'int_field': 99})[0]['_id'])
            client["simple_db"]["simple_coll_2"].update_one({'int_field': 99},{'$set': {'int_field': -1}})

            client["simple_db"]["simple_coll_2"].insert_one({"int_field": 100, "string_field": random_string_generator()})
            changed_ids.add(client['simple_db']['simple_coll_2'].find({'int_field': 100})[0]['_id'])

            client["simple_db"]["simple_coll_2"].insert_one({"int_field": 101, "string_field": random_string_generator()})
            changed_ids.add(client['simple_db']['simple_coll_2'].find({'int_field': 101})[0]['_id'])

        #  -----------------------------------
        # ----------- Subsequent Oplog Sync ---------
        #  -----------------------------------

        # Run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()
        records_by_stream = {
            'simple_coll_1': [x
                              for x in messages_by_stream['simple_coll_1']['messages']
                              if x.get('action') == 'upsert']
        }

        # assert that each of the streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        # 1 record due to fencepost querying on oplog ts
        self.assertEqual(1, record_count_by_stream['simple_coll_1'])

        final_state = menagerie.get_state(conn_id)

        # Verify that the bookmark timestamp exists and has reasonable values
        bookmark_time = final_state['bookmarks']['simple_db-simple_coll_1']['oplog_ts_time']
        bookmark_inc = final_state['bookmarks']['simple_db-simple_coll_1']['oplog_ts_inc']
        
        # Verify that the bookmark values are valid timestamps
        self.assertIsInstance(bookmark_time, int)
        self.assertIsInstance(bookmark_inc, int)
        self.assertGreater(bookmark_time, 0)
        self.assertGreater(bookmark_inc, 0)
        
        # Verify that the bookmark is recent (within a reasonable timeframe)
        current_time = int(time.time())
        # The bookmark should not be more than 1 hour old or in the future
        self.assertLessEqual(bookmark_time, current_time, "Bookmark timestamp should not be in the future")
        self.assertGreater(bookmark_time, current_time - 3600, "Bookmark timestamp should be within the last hour")
        
        # Verify that the bookmark was updated from the initial sync
        # The bookmark should be greater than or equal to the initial bookmark
        self.assertGreaterEqual(bookmark_time, initial_oplog_ts_time, 
                               "Final bookmark timestamp should be >= initial bookmark timestamp")
        
        # If the timestamp is the same, the increment should be greater or equal
        if bookmark_time == initial_oplog_ts_time:
            self.assertGreaterEqual(bookmark_inc, initial_oplog_ts_inc,
                                   "If timestamp is same, increment should be >= initial increment")
