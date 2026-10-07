import struct
import unittest

import bson
import pymongo
from bson.errors import InvalidBSON

# Binary element "leak" declaring 5 more bytes than it carries, from SAC-32067.
# PyMongo < 4.18.0 counted the 4-byte length and 1-byte subtype as payload and
# copied 5 bytes of adjacent heap memory into the decoded value.
_PAYLOAD = b"A" * 7
_ELEMENT = (b"\x05" + b"leak\x00" + struct.pack("<i", len(_PAYLOAD) + 5)
            + b"\x00" + _PAYLOAD)
OVERLONG_BINARY_DOCUMENT = (struct.pack("<i", len(_ELEMENT) + 5) + _ELEMENT
                            + b"\x00")


class TestBinaryOverRead(unittest.TestCase):

    def test_pymongo_contains_the_fix(self):
        self.assertGreaterEqual(pymongo.version_tuple[:3], (4, 18, 0))

    def test_overlong_binary_length_is_rejected(self):
        with self.assertRaises(InvalidBSON):
            bson.decode(OVERLONG_BINARY_DOCUMENT)
