import unittest
from unittest.mock import MagicMock, patch, call
from bson import timestamp
from pymongo.errors import ConfigurationError

import singer

import tap_mongodb.sync_strategies.oplog as oplog
import tap_mongodb.sync_strategies.common as common


TAP_STREAM_ID = 'testdb-testcoll'


def make_stream():
    return {
        'tap_stream_id': TAP_STREAM_ID,
        'stream': 'testcoll',
        'table_name': 'testcoll',
        'metadata': [
            {
                'breadcrumb': [],
                'metadata': {
                    'database-name': 'testdb'
                }
            }
        ]
    }


def make_state(oplog_ts_time=1000000, oplog_ts_inc=1, version=1000000000):
    return {
        'bookmarks': {
            TAP_STREAM_ID: {
                'oplog_ts_time': oplog_ts_time,
                'oplog_ts_inc': oplog_ts_inc,
                'version': version,
                'initial_full_table_complete': True
            }
        }
    }


def make_apply_ops_row(ops, ts_time=1000001):
    """Create a fake admin.$cmd applyOps oplog row (transactional operation)."""
    return {
        'ts': timestamp.Timestamp(ts_time, 1),
        'ns': 'admin.$cmd',
        'op': 'c',
        'o': {
            'applyOps': ops
        }
    }


def run_sync_collection(stream_projection, oplog_rows):
    """
    Run sync_collection with a mocked MongoDB client.
    Returns the list of singer messages that were written.
    """
    stream = make_stream()
    state = make_state()
    max_oplog_ts = timestamp.Timestamp(1000002, 1)

    # Initialise common module counters for the stream
    common.SCHEMA_COUNT[TAP_STREAM_ID] = 0
    common.SCHEMA_TIMES[TAP_STREAM_ID] = 0
    common.COUNTS[TAP_STREAM_ID] = 0
    common.TIMES[TAP_STREAM_ID] = 0

    # Build cursor mock: context manager wrapping an iterable of rows
    mock_cursor = MagicMock()
    mock_cursor.__iter__ = MagicMock(return_value=iter(oplog_rows))
    find_cm = MagicMock()
    find_cm.__enter__ = MagicMock(return_value=mock_cursor)
    find_cm.__exit__ = MagicMock(return_value=False)

    mock_client = MagicMock()
    # Force SessionNotAvailable path (no active session)
    mock_client.start_session.side_effect = ConfigurationError("no session")
    mock_client.local.oplog.rs.find.return_value = find_cm

    written = []

    def capture(msg, **kwargs):
        written.append(msg)

    with patch('tap_mongodb.sync_strategies.oplog.singer.write_message',
               side_effect=capture):
        oplog.sync_collection(mock_client, stream, state, stream_projection,
                              max_oplog_ts=max_oplog_ts)

    return written


def get_records(messages):
    return [m.record for m in messages if isinstance(m, singer.RecordMessage)]


class TestTransactionalInsertProjection(unittest.TestCase):
    """
    Unit tests for PR #134: apply projection to transactional insert ops in oplog sync.

    When documents are inserted inside a MongoDB transaction, the oplog records the
    operation as an applyOps command under admin.$cmd.  MongoDB cannot sub-project
    into the applyOps array, so the full document is returned.  The fix manually
    applies stream_projection to insert ops (op='i') inside applyOps so unprojected
    fields never reach write_schema or row_to_singer_record.
    """

    def test_projection_applied_to_transactional_insert(self):
        """Projected fields are included; non-projected fields are excluded."""
        ops = [
            {
                'op': 'i',
                'ns': 'testdb.testcoll',
                'o': {'_id': 'id1', 'string_field': 'hello', 'int_field': 999}
            }
        ]
        messages = run_sync_collection({'string_field': 1}, [make_apply_ops_row(ops)])
        records = get_records(messages)

        self.assertEqual(len(records), 1)
        self.assertIn('string_field', records[0])
        self.assertNotIn('int_field', records[0],
                         "int_field must be excluded by the projection")

    def test_id_always_preserved_in_projected_transactional_insert(self):
        """_id must always be present in the record even if not explicitly in the projection."""
        ops = [
            {
                'op': 'i',
                'ns': 'testdb.testcoll',
                'o': {'_id': 'abc123', 'string_field': 'hello', 'int_field': 999}
            }
        ]
        messages = run_sync_collection({'string_field': 1}, [make_apply_ops_row(ops)])
        records = get_records(messages)

        self.assertEqual(len(records), 1)
        self.assertIn('_id', records[0], "_id must always be preserved")

    def test_multiple_inserts_in_one_transaction(self):
        """All insert ops inside a single applyOps entry are projected correctly."""
        ops = [
            {'op': 'i', 'ns': 'testdb.testcoll',
             'o': {'_id': 'id1', 'string_field': 'TXN_1', 'int_field': 111}},
            {'op': 'i', 'ns': 'testdb.testcoll',
             'o': {'_id': 'id2', 'string_field': 'TXN_2', 'int_field': 222}},
        ]
        messages = run_sync_collection({'string_field': 1}, [make_apply_ops_row(ops)])
        records = get_records(messages)

        self.assertEqual(len(records), 2)
        for record in records:
            self.assertIn('string_field', record)
            self.assertNotIn('int_field', record,
                             "int_field must be excluded by the projection")

    def test_no_projection_passes_all_fields_through(self):
        """When stream_projection is None, all fields must be passed through unfiltered."""
        ops = [
            {
                'op': 'i',
                'ns': 'testdb.testcoll',
                'o': {'_id': 'id1', 'string_field': 'hello', 'int_field': 999}
            }
        ]
        messages = run_sync_collection(None, [make_apply_ops_row(ops)])
        records = get_records(messages)

        self.assertEqual(len(records), 1)
        self.assertIn('string_field', records[0])
        self.assertIn('int_field', records[0],
                      "Without a projection all fields should be present")

    def test_projection_not_applied_to_transactional_update(self):
        """
        Update ops (op='u') inside applyOps must NOT be filtered — they go into
        the update_buffer and are re-fetched via flush_buffer using stream_projection.
        """
        ops = [
            {
                'op': 'u',
                'ns': 'testdb.testcoll',
                'o': {'$set': {'string_field': 'updated'}},
                'o2': {'_id': 'id1'}
            }
        ]
        messages = run_sync_collection({'string_field': 1}, [make_apply_ops_row(ops)])
        records = get_records(messages)

        # Updates go into update_buffer — no record is written directly
        self.assertEqual(len(records), 0,
                         "Update ops must not emit records directly; they go to update_buffer")


class TestTransformProjection(unittest.TestCase):
    """Unit tests for the transform_projection helper."""

    def test_none_projection_returns_full_o(self):
        result = oplog.transform_projection(None)
        self.assertIn('o', result)
        self.assertNotIn('o.applyOps', result)

    def test_whitelist_projection_includes_applyOps(self):
        result = oplog.transform_projection({'string_field': 1})
        self.assertIn('o.applyOps', result,
                      "applyOps must be included so transactional rows are returned")
        self.assertIn('o.string_field', result)
        self.assertIn('o._id', result)

    def test_whitelist_projection_excludes_unspecified_fields(self):
        result = oplog.transform_projection({'string_field': 1})
        self.assertNotIn('o.int_field', result)


if __name__ == '__main__':
    unittest.main()
