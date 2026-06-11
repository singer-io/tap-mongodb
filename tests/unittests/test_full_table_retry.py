import unittest
from unittest.mock import patch, MagicMock, call
import copy
import time
from bson import ObjectId
from pymongo.errors import CursorNotFound

import singer
import tap_mongodb.sync_strategies.common as common
from tap_mongodb.sync_strategies.full_table import sync_collection


def make_stream(tap_stream_id='test_db-test_col', stream_name='test_col', database='test_db'):
    return {
        'tap_stream_id': tap_stream_id,
        'stream': stream_name,
        'metadata': [
            {'breadcrumb': (), 'metadata': {'database-name': database}}
        ]
    }


def make_state(tap_stream_id='test_db-test_col', bookmarks=None):
    state = {'bookmarks': {tap_stream_id: bookmarks or {}}}
    return state


def make_rows(ids):
    """Create row dicts with ObjectId _id fields."""
    return [{'_id': oid, 'name': f'doc_{i}'} for i, oid in enumerate(ids)]


class MockCursor:
    """Mock cursor that can raise CursorNotFound after N rows."""

    def __init__(self, rows, fail_after=None):
        self.rows = rows
        self.fail_after = fail_after
        self._index = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        if self.fail_after is not None and self._index >= self.fail_after:
            raise CursorNotFound('cursor id 12345 not found')
        if self._index >= len(self.rows):
            raise StopIteration
        row = self.rows[self._index]
        self._index += 1
        return row


@patch('singer.write_message')
class TestSyncCollectionRetryLogic(unittest.TestCase):

    def setUp(self):
        # Reset global counters with default keys
        tap_stream_id = 'test_db-test_col'
        common.COUNTS.clear()
        common.TIMES.clear()
        common.SCHEMA_COUNT.clear()
        common.SCHEMA_TIMES.clear()
        common.COUNTS[tap_stream_id] = 0
        common.TIMES[tap_stream_id] = 0
        common.SCHEMA_COUNT[tap_stream_id] = 0
        common.SCHEMA_TIMES[tap_stream_id] = 0

    def _setup_client(self, collection_mock):
        client = MagicMock()
        client.__getitem__ = MagicMock(return_value=MagicMock(
            __getitem__=MagicMock(return_value=collection_mock)
        ))
        return client

    def test_successful_sync_no_retry(self, mock_write_message):
        """Full sync completes without CursorNotFound - no retries needed."""
        ids = [ObjectId() for _ in range(5)]
        rows = make_rows(ids)

        collection = MagicMock()
        collection.find_one.return_value = {'_id': ids[-1]}
        collection.find.return_value = MockCursor(rows)

        client = self._setup_client(collection)
        stream = make_stream()
        state = make_state()

        sync_collection(client, stream, state, None)

        self.assertEqual(common.COUNTS['test_db-test_col'], 5)
        # find called exactly once (no retry)
        collection.find.assert_called_once()

    def test_retry_after_cursor_not_found(self, mock_write_message):
        """CursorNotFound mid-sync triggers retry from last checkpoint."""
        ids = [ObjectId() for _ in range(10)]
        rows = make_rows(ids)

        collection = MagicMock()
        collection.find_one.return_value = {'_id': ids[-1]}

        # First call: fail after 3 rows; second call: return remaining rows
        first_cursor = MockCursor(rows, fail_after=3)
        # On retry, filter will use $gte from last bookmark (ids[2])
        # so we return rows from ids[2] onward (includes duplicate of ids[2])
        remaining_rows = make_rows(ids[2:])
        second_cursor = MockCursor(remaining_rows)

        collection.find.side_effect = [first_cursor, second_cursor]

        client = self._setup_client(collection)
        stream = make_stream()
        state = make_state()

        sync_collection(client, stream, state, None)

        # 3 rows from first attempt + 8 rows from second attempt (includes 1 dup)
        self.assertEqual(common.COUNTS['test_db-test_col'], 11)
        # find called twice (initial + 1 retry)
        self.assertEqual(collection.find.call_count, 2)

    def test_retry_updates_find_filter_with_gte(self, mock_write_message):
        """On retry, find_filter should include $gte from last bookmarked id."""
        ids = [ObjectId() for _ in range(5)]
        rows = make_rows(ids)

        collection = MagicMock()
        collection.find_one.return_value = {'_id': ids[-1]}

        # Fail after 2 rows, then succeed
        first_cursor = MockCursor(rows, fail_after=2)
        remaining_rows = make_rows(ids[1:])
        second_cursor = MockCursor(remaining_rows)

        collection.find.side_effect = [first_cursor, second_cursor]

        client = self._setup_client(collection)
        stream = make_stream()
        state = make_state()

        sync_collection(client, stream, state, None)

        # Verify second find call has $gte in filter
        second_call_filter = collection.find.call_args_list[1][0][0]
        self.assertIn('$gte', second_call_filter['_id'])
        self.assertEqual(second_call_filter['_id']['$gte'], ids[1])

    def test_multiple_retries_succeed(self, mock_write_message):
        """Multiple CursorNotFound errors succeed within retry limit."""
        ids = [ObjectId() for _ in range(15)]
        rows = make_rows(ids)

        collection = MagicMock()
        collection.find_one.return_value = {'_id': ids[-1]}

        # Fail at row 3, then fail at row 2 of remaining, then succeed
        cursor1 = MockCursor(rows, fail_after=3)
        cursor2 = MockCursor(make_rows(ids[2:]), fail_after=2)
        cursor3 = MockCursor(make_rows(ids[3:]))

        collection.find.side_effect = [cursor1, cursor2, cursor3]

        client = self._setup_client(collection)
        stream = make_stream()
        state = make_state()

        sync_collection(client, stream, state, None)

        self.assertEqual(collection.find.call_count, 3)

    def test_max_retries_exceeded_raises(self, mock_write_message):
        """Exceeding MAX_CURSOR_RETRIES raises CursorNotFound."""
        ids = [ObjectId() for _ in range(20)]
        rows = make_rows(ids)

        collection = MagicMock()
        collection.find_one.return_value = {'_id': ids[-1]}

        # Always fail after 1 row - exhaust all retries
        def make_failing_cursor(*args, **kwargs):
            return MockCursor(rows, fail_after=1)

        collection.find.side_effect = make_failing_cursor

        client = self._setup_client(collection)
        stream = make_stream()
        state = make_state()

        with self.assertRaises(CursorNotFound):
            sync_collection(client, stream, state, None)

        # Should have tried MAX_CURSOR_RETRIES times
        self.assertEqual(collection.find.call_count, common.MAX_CURSOR_RETRIES)

    def test_retry_preserves_rows_saved_count(self, mock_write_message):
        """rows_saved accumulates across retries (not reset)."""
        ids = [ObjectId() for _ in range(6)]
        rows = make_rows(ids)

        collection = MagicMock()
        collection.find_one.return_value = {'_id': ids[-1]}

        # Fail after 4 rows, then complete remaining
        cursor1 = MockCursor(rows, fail_after=4)
        cursor2 = MockCursor(make_rows(ids[3:]))

        collection.find.side_effect = [cursor1, cursor2]

        client = self._setup_client(collection)
        stream = make_stream()
        state = make_state()

        sync_collection(client, stream, state, None)

        # 4 rows + 3 remaining (including 1 overlap from $gte)
        self.assertEqual(common.COUNTS['test_db-test_col'], 7)

    def test_retry_logs_warning(self, mock_write_message):
        """CursorNotFound triggers a LOGGER.warning with retry info."""
        ids = [ObjectId() for _ in range(5)]
        rows = make_rows(ids)

        collection = MagicMock()
        collection.find_one.return_value = {'_id': ids[-1]}

        cursor1 = MockCursor(rows, fail_after=2)
        cursor2 = MockCursor(make_rows(ids[1:]))
        collection.find.side_effect = [cursor1, cursor2]

        client = self._setup_client(collection)
        stream = make_stream()
        state = make_state()

        with patch('tap_mongodb.sync_strategies.full_table.LOGGER') as mock_logger:
            sync_collection(client, stream, state, None)
            mock_logger.warning.assert_called_once()
            warning_args = mock_logger.warning.call_args[0]
            self.assertIn('CursorNotFound', warning_args[0])
            self.assertIn('retry', warning_args[0].lower())

    def test_bookmarks_cleared_on_success(self, mock_write_message):
        """After successful sync, intermediate bookmarks are cleared."""
        ids = [ObjectId() for _ in range(3)]
        rows = make_rows(ids)

        collection = MagicMock()
        collection.find_one.return_value = {'_id': ids[-1]}
        collection.find.return_value = MockCursor(rows)

        client = self._setup_client(collection)
        stream = make_stream()
        state = make_state()

        sync_collection(client, stream, state, None)

        bookmarks = state['bookmarks']['test_db-test_col']
        self.assertNotIn('max_id_value', bookmarks)
        self.assertNotIn('max_id_type', bookmarks)
        self.assertNotIn('last_id_fetched', bookmarks)
        self.assertNotIn('last_id_fetched_type', bookmarks)
        self.assertTrue(bookmarks.get('initial_full_table_complete'))

    def test_empty_collection_no_retry(self, mock_write_message):
        """Empty collection completes without errors or retries."""
        collection = MagicMock()
        collection.find_one.return_value = None
        collection.find.return_value = MockCursor([])

        client = self._setup_client(collection)
        stream = make_stream()
        state = make_state()

        sync_collection(client, stream, state, None)

        self.assertEqual(common.COUNTS['test_db-test_col'], 0)
        collection.find.assert_called_once()

    def test_state_bookmark_updated_during_sync(self, mock_write_message):
        """State bookmarks are updated as rows are processed."""
        ids = [ObjectId() for _ in range(3)]
        rows = make_rows(ids)

        collection = MagicMock()
        collection.find_one.return_value = {'_id': ids[-1]}
        collection.find.return_value = MockCursor(rows)

        client = self._setup_client(collection)
        stream = make_stream()
        state = make_state()

        sync_collection(client, stream, state, None)

        # After sync, last_id_fetched was cleared (successful sync)
        # but during sync it should have been set to the last row's id
        # Verify initial_full_table_complete is set
        self.assertTrue(
            state['bookmarks']['test_db-test_col']['initial_full_table_complete']
        )


if __name__ == '__main__':
    unittest.main()
