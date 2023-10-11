import os
import random
import string
import unittest
from bson import ObjectId
from pymongo.errors import OperationFailure

from mongodb_common import drop_all_collections, get_test_connection, ensure_environment_variables_set
from tap_tester import connections, menagerie, runner, LOGGER


RECORD_COUNT = {}

# namespace length = db name + collection name + 1 (for the separator dot)
#   mongo 4.2 max supported namespace is 120 bytes = 120 characters
#   mongo 4.4, 5.0, and 6.0 max supported namespace is 255 bytes
db_name = 'simple_db'
max_name_length = 120

# collection names
coll_name_1 = f'{"Collection_name_with_110_characters_":1<110}'
coll_name_2 = f'{"Collection_name_with_110_characters_":2<110}'
coll_name_3 = f'{"Collection_name_with_111_characters_":3<111}'

coll_name_4 = f'{"Collection_name_with_245_characters_":4<245}'
coll_name_5 = f'{"Collection_name_with_245_characters_":5<245}'
coll_name_6 = f'{"Collection_name_with_246_characters_":6<246}'


def random_string_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for x in range(size))

def generate_simple_coll_docs(num_docs):
    docs = []
    for int_value in range(num_docs):
        docs.append({"int_field": int_value, "string_field": random_string_generator()})
    return docs

class MongoDBNameSpaceRestrictions(unittest.TestCase):
    ''' Test edge case namespace restrictions per the documentation (120 byte max)
        Reference https://jira.talendforge.org/browse/TDL-18990 for details  '''

    def setUp(self):

        ensure_environment_variables_set()

        with get_test_connection() as client:
            ############# Drop all dbs/collections #############
            drop_all_collections(client)

            db_version = client.server_info()['version']
            LOGGER.info("MongoDB version: {}".format(db_version))

            if db_version.startswith('4.2.'):  # 4.2-bionic db version string unknown BUG
                self.coll_name_1 = coll_name_1
                self.coll_name_2 = coll_name_2
                self.coll_name_3 = coll_name_3
                self.max_name_length = max_name_length
            elif db_version in ['4.4.6', '5.0.15', '6.0.4']:
                self.coll_name_1 = coll_name_4
                self.coll_name_2 = coll_name_5
                self.coll_name_3 = coll_name_6
                self.max_name_length = 255
            else:
                assert True == False, "Uknown database version detected!"

            LOGGER.info("Namespace lengths. 1: {}, 2: {}, 3: {}, Max: {}".format(
                len(self.coll_name_1) + len(db_name) + 1,
                len(self.coll_name_2) + len(db_name) + 1,
                len(self.coll_name_3) + len(db_name) + 1,
                self.max_name_length))

            ############# Add simple collections #############
            # coll_name_1 has 50 documents
            client[db_name][self.coll_name_1].insert_many(generate_simple_coll_docs(50))

            # coll_name_2 has 100 documents
            client[db_name][self.coll_name_2].insert_many(generate_simple_coll_docs(100))

            # coll_name_3 should fail
            with self.assertRaises(OperationFailure) as e:
                client[db_name][self.coll_name_3].insert_many(generate_simple_coll_docs(10))

            self.assertIn("Fully qualified namespace is too long", str(e.exception.details.values()))
            self.assertIn(f"Max: {self.max_name_length}", str(e.exception.details.values()))


    def expected_check_streams(self):
        return {
            f'{db_name}-{self.coll_name_1}',
            f'{db_name}-{self.coll_name_2}',
        }

    def expected_pks(self):
        return {
            self.coll_name_1: {'_id'},
            self.coll_name_2: {'_id'},
        }

    def expected_row_counts(self):
        return {
            self.coll_name_1: 50,
            self.coll_name_2: 100,
        }

    def expected_sync_streams(self):
        return {
            self.coll_name_1,
            self.coll_name_2
        }

    def name(self):
        return "tap_tester_mongodb_namespace_restrict"

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

        #  ---------------------------------
        #  -----------  Discovery ----------
        #  ---------------------------------

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

        #  ----------------------------------------
        #  ----------- Initial Full Table ---------
        #  ----------------------------------------

        # Select coll_name_1 and coll_name_2 streams and add replication method metadata
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

        # Verify that the full table was synced
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

        changed_ids = set()
        with get_test_connection() as client:
            # Delete two documents for each collection

            changed_ids.add(client[db_name][self.coll_name_1].find({'int_field': 0})[0]['_id'])
            client[db_name][self.coll_name_1].delete_one({'int_field': 0})

            changed_ids.add(client[db_name][self.coll_name_1].find({'int_field': 1})[0]['_id'])
            client[db_name][self.coll_name_1].delete_one({'int_field': 1})

            changed_ids.add(client[db_name][self.coll_name_2].find({'int_field': 0})[0]['_id'])
            client[db_name][self.coll_name_2].delete_one({'int_field': 0})

            changed_ids.add(client[db_name][self.coll_name_2].find({'int_field': 1})[0]['_id'])
            client[db_name][self.coll_name_2].delete_one({'int_field': 1})

            # Update two documents for each collection
            changed_ids.add(client[db_name][self.coll_name_1].find({'int_field': 48})[0]['_id'])
            client[db_name][self.coll_name_1].update_one({'int_field': 48},{'$set': {'int_field': -1}})

            changed_ids.add(client[db_name][self.coll_name_1].find({'int_field': 49})[0]['_id'])
            client[db_name][self.coll_name_1].update_one({'int_field': 49},{'$set': {'int_field': -1}})

            changed_ids.add(client[db_name][self.coll_name_2].find({'int_field': 98})[0]['_id'])
            client[db_name][self.coll_name_2].update_one({'int_field': 98},{'$set': {'int_field': -1}})

            changed_ids.add(client[db_name][self.coll_name_2].find({'int_field': 99})[0]['_id'])
            client[db_name][self.coll_name_2].update_one({'int_field': 99},{'$set': {'int_field': -1}})

            # Insert two documents for each collection
            client[db_name][self.coll_name_1].insert_one({"int_field": 50, "string_field": random_string_generator()})
            changed_ids.add(client[db_name][self.coll_name_1].find({'int_field': 50})[0]['_id'])

            client[db_name][self.coll_name_1].insert_one({"int_field": 51, "string_field": random_string_generator()})
            changed_ids.add(client[db_name][self.coll_name_1].find({'int_field': 51})[0]['_id'])

            client[db_name][self.coll_name_2].insert_one({"int_field": 100, "string_field": random_string_generator()})
            changed_ids.add(client[db_name][self.coll_name_2].find({'int_field': 100})[0]['_id'])

            client[db_name][self.coll_name_2].insert_one({"int_field": 101, "string_field": random_string_generator()})
            changed_ids.add(client[db_name][self.coll_name_2].find({'int_field': 101})[0]['_id'])

        #  -------------------------------------------
        #  ----------- Subsequent Oplog Sync ---------
        #  -------------------------------------------

        # Run sync
        sync_job_name = runner.run_sync_mode(self, conn_id)

        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # verify the persisted schema was correct
        messages_by_stream = runner.get_records_from_target_output()
        records_by_stream = {}
        for stream_name in self.expected_sync_streams():
            records_by_stream[stream_name] = [x for x in messages_by_stream[stream_name]['messages']
                                              if x.get('action') == 'upsert']

        # assert that each of the streams that we synced are the ones that we expect to see
        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        # Verify that we got at least 6 records due to changes
        # (could be more due to overlap in gte oplog clause)
        for k,v in record_count_by_stream.items():
            self.assertGreaterEqual(v, 6)

        for stream in self.expected_sync_streams():
            # Verify that we got 2 records with _SDC_DELETED_AT
            self.assertEqual(2, len([x['data'] for x in records_by_stream[stream]
                                     if x['data'].get('_sdc_deleted_at')]))

        # Verify that the _id of the records sent are the same set as the
        # _ids of the documents changed
        actual_ids = {ObjectId(x['data']['_id']) for stream in self.expected_sync_streams()
                      for x in records_by_stream[stream]}

        self.assertEqual(changed_ids, actual_ids)
