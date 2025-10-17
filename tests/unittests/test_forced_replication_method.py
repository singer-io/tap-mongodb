import unittest
from unittest.mock import Mock, MagicMock
import pymongo
from singer import metadata

import tap_mongodb


class TestForcedReplicationMethod(unittest.TestCase):
    """Test that forced-replication-method is correctly set in discover catalog."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock database and collection
        self.mock_db = Mock()
        self.mock_collection = Mock()
        self.mock_collection.name = "test_collection"
        self.mock_collection.database = self.mock_db
        self.mock_db.name = "test_db"
        
    def test_forced_replication_method_full_table_no_indexes(self):
        """Test that collections without any valid indexes get FULL_TABLE."""
        # Mock collection with no valid indexes (edge case)
        self.mock_collection.options.return_value = {}  # Not a view
        self.mock_collection.estimated_document_count.return_value = 100
        self.mock_collection.index_information.return_value = {}  # No indexes at all
        
        schema = tap_mongodb.produce_collection_schema(self.mock_collection)
        
        # Extract metadata
        mdata = metadata.to_map(schema['metadata'])
        forced_replication_method = metadata.get(mdata, (), 'forced-replication-method')
        valid_replication_keys = metadata.get(mdata, (), 'valid-replication-keys')
        
        # Assertions
        self.assertEqual(forced_replication_method, 'FULL_TABLE')
        self.assertEqual(valid_replication_keys, None)  # No valid replication keys
        
    def test_forced_replication_method_incremental_with_id_only(self):
        """Test that collections with just _id index get INCREMENTAL."""
        # Mock collection with only default _id index (most common case)
        self.mock_collection.options.return_value = {}  # Not a view
        self.mock_collection.estimated_document_count.return_value = 100
        self.mock_collection.index_information.return_value = {
            '_id_': {'key': [('_id', 1)]}  # Only default _id index
        }
        
        schema = tap_mongodb.produce_collection_schema(self.mock_collection)
        
        # Extract metadata
        mdata = metadata.to_map(schema['metadata'])
        forced_replication_method = metadata.get(mdata, (), 'forced-replication-method')
        valid_replication_keys = metadata.get(mdata, (), 'valid-replication-keys')
        
        # Assertions
        self.assertEqual(forced_replication_method, 'INCREMENTAL')
        self.assertEqual(valid_replication_keys, ['_id'])

    def test_forced_replication_method_incremental_with_additional_indexes(self):
        """Test that collections with additional indexes get INCREMENTAL."""
        # Mock collection with additional indexes
        self.mock_collection.options.return_value = {}  # Not a view
        self.mock_collection.estimated_document_count.return_value = 100
        self.mock_collection.index_information.return_value = {
            '_id_': {'key': [('_id', 1)]},  # Default _id index
            'date_field_1': {'key': [('date_field', 1)]},  # Additional index
            'name_field_1': {'key': [('name_field', -1)]},  # Another additional index
        }
        
        schema = tap_mongodb.produce_collection_schema(self.mock_collection)
        
        # Extract metadata
        mdata = metadata.to_map(schema['metadata'])
        forced_replication_method = metadata.get(mdata, (), 'forced-replication-method')
        valid_replication_keys = metadata.get(mdata, (), 'valid-replication-keys')
        
        # Assertions
        self.assertEqual(forced_replication_method, 'INCREMENTAL')
        self.assertIn('_id', valid_replication_keys)
        self.assertIn('date_field', valid_replication_keys)
        self.assertIn('name_field', valid_replication_keys)
        
    def test_forced_replication_method_compound_indexes_ignored(self):
        """Test that compound indexes are ignored and only single-field indexes count."""
        # Mock collection with compound indexes (should be ignored)
        self.mock_collection.options.return_value = {}  # Not a view
        self.mock_collection.estimated_document_count.return_value = 100
        self.mock_collection.index_information.return_value = {
            '_id_': {'key': [('_id', 1)]},  # Default _id index
            'compound_1': {'key': [('field1', 1), ('field2', -1)]},  # Compound index (ignored)
            'single_field_1': {'key': [('single_field', 1)]},  # Single field index
        }
        
        schema = tap_mongodb.produce_collection_schema(self.mock_collection)
        
        # Extract metadata
        mdata = metadata.to_map(schema['metadata'])
        forced_replication_method = metadata.get(mdata, (), 'forced-replication-method')
        valid_replication_keys = metadata.get(mdata, (), 'valid-replication-keys')
        
        # Assertions
        self.assertEqual(forced_replication_method, 'INCREMENTAL')
        self.assertIn('_id', valid_replication_keys)
        self.assertIn('single_field', valid_replication_keys)
        # Compound index fields should not be included
        self.assertNotIn('field1', valid_replication_keys)
        self.assertNotIn('field2', valid_replication_keys)
        
    def test_views_no_forced_replication_method(self):
        """Test that views don't get forced-replication-method metadata."""
        # Mock view collection
        self.mock_collection.options.return_value = {'viewOn': 'base_collection'}  # Is a view
        self.mock_collection.estimated_document_count.return_value = 50
        
        schema = tap_mongodb.produce_collection_schema(self.mock_collection)
        
        # Extract metadata
        mdata = metadata.to_map(schema['metadata'])
        forced_replication_method = metadata.get(mdata, (), 'forced-replication-method')
        
        # Assertions - views should not have forced-replication-method
        self.assertIsNone(forced_replication_method)
        
    def test_schema_structure_unchanged(self):
        """Test that the overall schema structure remains the same."""
        # Mock collection
        self.mock_collection.options.return_value = {}  # Not a view
        self.mock_collection.estimated_document_count.return_value = 75
        self.mock_collection.index_information.return_value = {
            '_id_': {'key': [('_id', 1)]},
        }
        
        schema = tap_mongodb.produce_collection_schema(self.mock_collection)
        
        # Test schema structure
        expected_keys = {'table_name', 'stream', 'metadata', 'tap_stream_id', 'schema'}
        self.assertEqual(set(schema.keys()), expected_keys)
        self.assertEqual(schema['table_name'], 'test_collection')
        self.assertEqual(schema['stream'], 'test_collection')  
        self.assertEqual(schema['tap_stream_id'], 'test_db-test_collection')
        self.assertEqual(schema['schema'], {'type': 'object'})
        
        # Test that all expected metadata is present
        mdata = metadata.to_map(schema['metadata'])
        self.assertEqual(metadata.get(mdata, (), 'table-key-properties'), ['_id'])
        self.assertEqual(metadata.get(mdata, (), 'database-name'), 'test_db')
        self.assertEqual(metadata.get(mdata, (), 'row-count'), 75)
        self.assertEqual(metadata.get(mdata, (), 'is-view'), False)
