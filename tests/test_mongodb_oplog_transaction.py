import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import unittest
import string
import random
from mongodb_common import drop_all_collections, get_test_connection

RECORD_COUNT = {}

def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def generate_simple_coll_docs(num_docs):
    docs = []
    for int_value in range(num_docs):
        docs.append({"int_field": int_value, "string_field": random_string_generator()})
    return docs

class MongoDBOplog(unittest.TestCase):
    table_name = 'collection_with_transaction_1'
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
            client["simple_db"][self.table_name].insert_many(generate_simple_coll_docs(50))



    def expected_check_streams(self):
        return {
            'simple_db-collection_with_transaction_1',
        }

    def expected_pks(self):
        return {
            self.table_name: {'_id'}
        }

    def expected_row_counts(self):
        return {
            self.table_name: 50
        }

    def expected_sync_streams(self):
        return {
            self.table_name
        }

    def name(self):
        return "tap_tester_mongodb_oplog_with_transaction"

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
        # ----------- Initial Full Table ---------
        #  -----------------------------------
        # Select simple_db-collection_with_transaction_1 stream and add replication method metadata
        for stream_catalog in found_catalogs:
            annotated_schema = menagerie.get_annotated_schema(conn_id, stream_catalog['stream_id'])
            additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'LOG_BASED'}}]
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

        # Verify that the full table was syncd
        for tap_stream_id in self.expected_sync_streams():
            self.assertGreaterEqual(record_count_by_stream[tap_stream_id],self.expected_row_counts()[tap_stream_id])

        # Verify that we have 'initial_full_table_complete' bookmark
        state = menagerie.get_state(conn_id)
        first_versions = {}

        for tap_stream_id in self.expected_check_streams():
            # assert that the state has an initial_full_table_complete == True
            self.assertTrue(state['bookmarks'][tap_stream_id]['initial_full_table_complete'])
            # assert that there is a version bookmark in state
            first_versions[tap_stream_id] = state['bookmarks'][tap_stream_id]['version']
            self.assertIsNotNone(first_versions[tap_stream_id])
            # Verify that we have a oplog_ts_time and oplog_ts_inc bookmark
            self.assertIsNotNone(state['bookmarks'][tap_stream_id]['oplog_ts_time'])
            self.assertIsNotNone(state['bookmarks'][tap_stream_id]['oplog_ts_inc'])

        # Create records for oplog sync
        with get_test_connection() as client:
            db = client['simple_db'][self.table_name]
            with client.start_session() as session:
                with session.start_transaction():
                    db.insert_many([{"int_field": x, "string_field": str(x)} for x in range(51, 61)], session=session)

            # Insert 10 docs in one transaction, update 5 of them
            with client.start_session() as session:
                with session.start_transaction():
                    db.insert_many([{"int_field": x, "string_field": str(x)} for x in range(61, 71)], session=session)
                    for x in range(61, 66):
                        db.update_one({"string_field": str(x)}, {"$inc": {"int_field": 1}}, session=session)

            # Update 5 docs in one transaction from the the first transaction
            with client.start_session() as session:
                with session.start_transaction():
                    for x in range(51, 56):
                        db.update_one({"string_field": str(x)}, {"$inc": {"int_field": 1}}, session=session)


        #  -----------------------------------
        # ----------- Subsequent Oplog Sync ---------
        #  -----------------------------------

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

        # Verify that we get 30 records due to the above transactions performed
        # (could be more due to overlap in gte oplog clause)
        self.assertGreaterEqual(31,
                         record_count_by_stream[self.table_name])
