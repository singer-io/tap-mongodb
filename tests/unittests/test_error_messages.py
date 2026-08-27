import logging
import struct
import unittest
from unittest.mock import patch

import bson
from bson.errors import InvalidBSON
from pymongo.errors import OperationFailure, ServerSelectionTimeoutError

import tap_mongodb
from tap_mongodb.error_messages import (MAX_MESSAGE_LENGTH, LineForgeryFilter,
                                        install_line_forgery_filter,
                                        neutralize_line_forgery,
                                        safe_error_message, scrub)


# Mirrors the payload from the Bugcrowd report: seven attacker supplied bytes,
# the BSON document terminator, then bytes the server never sent.
LEAKED_BYTES = b"AAAAAAA\x00\x00\x27\x57\xd9"


def _malformed_binary_document():
    """Build the malformed response from the report.

    A trailing BSON Binary element declares a 12 byte payload but only seven
    bytes follow it, so a driver that fails to account for the five byte
    length/subtype header copies bytes from past the end of the buffer.
    """
    element = b'\x05' + b'leak\x00' + struct.pack('<iB', 12, 0) + b'A' * 7
    body = element + b'\x00'

    return struct.pack('<i', 4 + len(body)) + body


class TestScrub(unittest.TestCase):

    def test_removes_bytes_literals(self):
        scrubbed = scrub("'leak': {}".format(LEAKED_BYTES))

        self.assertNotIn('\\x', scrubbed)
        self.assertNotIn('xd9', scrubbed)
        self.assertNotIn('AAAAAAA', scrubbed)
        self.assertIn('<redacted>', scrubbed)

    def test_removes_escape_sequences(self):
        for escaped in ['\\xd9', '\\u00d9', '\\U000000d9', '\\331', '\\N{BULLET}']:
            with self.subTest(escaped=escaped):
                scrubbed = scrub('leak={}'.format(escaped))

                self.assertEqual('leak=<redacted>', scrubbed)

    def test_collapses_newlines_so_critical_lines_cannot_be_forged(self):
        scrubbed = scrub('boom\nCRITICAL forged\r\nFATAL forged')

        self.assertEqual(1, len(scrubbed.splitlines()))
        self.assertFalse(scrubbed.startswith('CRITICAL '))

    def test_drops_backslashes(self):
        self.assertEqual('a<redacted>', scrub('a\\\\b'))

    def test_truncates_long_messages(self):
        scrubbed = scrub('z' * (MAX_MESSAGE_LENGTH * 3))

        self.assertEqual(MAX_MESSAGE_LENGTH + 3, len(scrubbed))
        self.assertTrue(scrubbed.endswith('...'))

    def test_keeps_ordinary_text_intact(self):
        self.assertEqual('collection simple_coll_1 (db: simple_db) is empty.',
                         scrub('collection simple_coll_1 (db: simple_db) is empty.'))


class TestSafeErrorMessage(unittest.TestCase):

    def test_operation_failure_does_not_reflect_server_response(self):
        exc = OperationFailure(
            "not authorized, full error: {{'ok': 0.0, 'errmsg': 'nope', 'leak': {}}}".format(
                LEAKED_BYTES),
            code=13)

        message = safe_error_message(exc)

        self.assertNotIn('AAAAAAA', message)
        self.assertNotIn('\\x', message)
        self.assertNotIn('xd9', message)
        self.assertNotIn('errmsg', message)
        self.assertNotIn('nope', message)
        self.assertIn('OperationFailure', message)
        self.assertIn('code 13', message)
        self.assertIn('not authorized to perform this operation', message)

    def test_operation_failure_without_a_code(self):
        message = safe_error_message(OperationFailure('leaky message'))

        self.assertNotIn('leaky message', message)
        self.assertIn('OperationFailure', message)

    def test_known_pymongo_errors_get_an_actionable_description(self):
        message = safe_error_message(ServerSelectionTimeoutError('10.0.0.1:27017 timed out'))

        self.assertNotIn('10.0.0.1', message)
        self.assertIn('Could not reach a usable MongoDB server', message)
        self.assertIn('ServerSelectionTimeoutError', message)

    def test_bson_errors_are_treated_as_untrusted(self):
        message = safe_error_message(InvalidBSON('invalid length or type code'))

        self.assertNotIn('invalid length or type code', message)
        self.assertIn('malformed BSON response', message)

    def test_tap_raised_errors_keep_a_scrubbed_message(self):
        message = safe_error_message(
            ValueError("Unrecognized replication_method b'\\xd9'\nCRITICAL forged"))

        self.assertTrue(message.startswith('ValueError: Unrecognized replication_method'))
        self.assertNotIn('xd9', message)
        self.assertNotIn('\nCRITICAL', message)
        self.assertEqual(1, len(message.splitlines()))

    def test_falls_back_to_the_exception_name_when_there_is_no_message(self):
        self.assertEqual('RuntimeError', safe_error_message(RuntimeError()))

    def test_malformed_binary_response_is_never_reflected(self):
        # Passes against both a fixed driver (which raises InvalidBSON) and a
        # driver whose native decoder still over-reads (which decodes the
        # element and hands the bytes to OperationFailure).
        try:
            decoded = bson.decode(_malformed_binary_document())
        except InvalidBSON as exc:
            message = safe_error_message(exc)
        else:
            message = safe_error_message(
                OperationFailure('failed, full error: {}'.format(decoded), code=8))

        self.assertNotIn('AAAAAAA', message)
        self.assertNotIn('\\x', message)
        self.assertNotIn('leak', message)


class TestMain(unittest.TestCase):

    @patch('tap_mongodb.main_impl')
    @patch('tap_mongodb.LOGGER')
    def test_main_logs_a_safe_message_and_exits_without_re_raising(self, logger, main_impl):
        main_impl.side_effect = OperationFailure(
            "Authentication failed, full error: {{'leak': {}}}".format(LEAKED_BYTES), code=18)

        with self.assertRaises(SystemExit) as context:
            tap_mongodb.main()

        self.assertEqual(1, context.exception.code)

        logger.critical.assert_called_once()
        logged = logger.critical.call_args[0][0]
        self.assertIsInstance(logged, str)
        self.assertNotIn('AAAAAAA', logged)
        self.assertNotIn('xd9', logged)
        self.assertIn('authentication failed', logged)


class TestLineForgery(unittest.TestCase):

    def test_indents_lines_that_could_be_read_as_a_new_record(self):
        neutralized = neutralize_line_forgery(
            'Starting full table sync for mydb-coll\nCRITICAL forged\nINFO METRIC: {}')

        self.assertEqual(
            'Starting full table sync for mydb-coll\n CRITICAL forged\n INFO METRIC: {}',
            neutralized)

    def test_normalizes_exotic_line_separators(self):
        for separator in ['\r', '\r\n', '\x0b', '\x0c', '\x1c', '\x85', '\u2028']:
            with self.subTest(separator=separator):
                neutralized = neutralize_line_forgery('coll{}FATAL forged'.format(separator))

                self.assertEqual('coll\n FATAL forged', neutralized)

    def test_leaves_deliberate_multi_line_messages_alone(self):
        message = 'Querying mydb-coll with:\n\tFind Parameters: {}'

        self.assertEqual(message, neutralize_line_forgery(message))

    def test_leaves_single_line_messages_untouched(self):
        self.assertEqual('CRITICAL-ish text', neutralize_line_forgery('CRITICAL-ish text'))

    def test_filter_rewrites_the_record(self):
        record = logging.LogRecord('tap-mongodb', logging.INFO, __file__, 1,
                                   'Starting full table sync for %s',
                                   ('mydb-coll\nCRITICAL forged',), None)

        self.assertTrue(LineForgeryFilter().filter(record))
        self.assertEqual('Starting full table sync for mydb-coll\n CRITICAL forged',
                         record.getMessage())

    def test_filter_does_not_reformat_untouched_records(self):
        record = logging.LogRecord('tap-mongodb', logging.INFO, __file__, 1,
                                   'Synced %s records for %s', (5, 'mydb-coll'), None)

        LineForgeryFilter().filter(record)

        self.assertEqual('Synced 5 records for mydb-coll', record.getMessage())

    def test_install_attaches_the_filter_to_root_handlers(self):
        root = logging.getLogger()
        handler = logging.NullHandler()
        root.addHandler(handler)
        try:
            log_filter = install_line_forgery_filter()

            self.assertIn(log_filter, handler.filters)
        finally:
            root.removeHandler(handler)
            for existing in list(root.handlers):
                existing.filters = [f for f in existing.filters
                                    if not isinstance(f, LineForgeryFilter)]


if __name__ == '__main__':
    unittest.main()
