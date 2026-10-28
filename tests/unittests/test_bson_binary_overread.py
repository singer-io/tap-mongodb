import struct
import unittest

import bson
import pymongo
from bson.errors import InvalidBSON
from bson.regex import Regex

# Binary element "leak" declaring 5 more bytes than it carries, from SAC-32067.
# PyMongo < 4.18.0 counted the 4-byte length and 1-byte subtype as payload and
# copied 5 bytes past the element into the decoded value.
_PAYLOAD = b"A" * 7
_ELEMENT = (b"\x05" + b"leak\x00" + struct.pack("<i", len(_PAYLOAD) + 5)
            + b"\x00" + _PAYLOAD)
OVERLONG_BINARY_DOCUMENT = (struct.pack("<i", len(_ELEMENT) + 5) + _ELEMENT
                            + b"\x00")


def unterminated_regex_document():
    # Regex element with no NUL after its flags (CVE-2026-106435). PyMongo
    # < 4.18.3 scanned past the end of the document looking for it.
    doc = bytearray(bson.encode({"a": Regex("x", "")}))
    del doc[-2:]
    doc[:4] = struct.pack("<i", len(doc))
    return bytes(doc)


class TestBsonOverRead(unittest.TestCase):

    def test_pymongo_contains_the_fixes(self):
        self.assertGreaterEqual(pymongo.version_tuple[:3], (4, 18, 3))

    def test_overlong_binary_length_is_rejected(self):
        with self.assertRaises(InvalidBSON):
            bson.decode(OVERLONG_BINARY_DOCUMENT)

    def test_unterminated_regex_is_rejected(self):
        with self.assertRaises(InvalidBSON):
            bson.decode(unterminated_regex_document())
