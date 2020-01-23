import unittest
import bson
import decimal

import tap_mongodb.sync_strategies.common as common

class TestRowToSchemaMessage(unittest.TestCase):
    def test_one(self):
        row = {"a_str": "hello"}
        schema = {"type": "object", "properties": {}}
        schema = common.row_to_schema_message(schema, row, row)

        self.assertEqual({'properties': {'a_str': {'type': 'string'}}, 'type': 'object'}, schema)

    def test_two(self):
        row = {"a_date": bson.timestamp.Timestamp(1565897157, 1)}
        schema = {"type": "object", "properties": {}}

        singer_row = {k:common.transform_value(v, [k]) for k, v in row.items()
                          if type(v) not in [bson.min_key.MinKey, bson.max_key.MaxKey]}
        changed = common.row_to_schema_message(schema, singer_row, row)
        self.assertEqual({'properties': {'a_date': {'type': 'string', 'format': 'date-time'}}, 'type': 'object'}, changed)

        import ipdb; ipdb.set_trace()
        1+1
        
    # def test_no_change(self):
    #     row = {"a_str": "hello"}
    #     schema = {"type": "object", "properties": {}}
    #     changed = common.row_to_schema_message(schema, row)
    #     self.assertFalse(changed)

    #     # another row that looks the same keeps changed false
    #     changed = common.row_to_schema_message(schema, row)
    #     self.assertFalse(changed)

    #     row = {"a_str": "hello",
    #            "a_date": bson.timestamp.Timestamp(1565897157, 1)}
    #     changed = common.row_to_schema_message(schema, row)
    #     # a different looking row makes the schema change
    #     self.assertTrue(changed)

    #     # the same (different) row again sets changed back to false
    #     changed = common.row_to_schema_message(schema, row)
    #     self.assertFalse(changed)


    # def test_simple_date(self):
    #     row = {"a_date": bson.timestamp.Timestamp(1565897157, 1)}
    #     schema = {"type": "object", "properties": {}}
    #     changed = common.row_to_schema_message(schema, row)

    #     expected = {"type": "object",
    #                 "properties": {
    #                     "a_date": {
    #                         "anyOf": [{"type": "string",
    #                                    "format": "date-time"},
    #                                   {}]
    #                     }
    #                 }
    #     }
    #     self.assertTrue(changed)
    #     self.assertEqual(expected, schema)


    # def test_simple_decimal(self):
    #     row = {"a_decimal": bson.Decimal128(decimal.Decimal('1.34'))}
    #     schema = {"type": "object", "properties": {}}
    #     changed = common.row_to_schema_message(schema, row)

    #     expected = {"type": "object",
    #                 "properties": {
    #                     "a_decimal": {
    #                         "anyOf": [{"type": "number",
    #                                    "multipleOf": 1e-34},
    #                                   {}]
    #                     }
    #                 }
    #     }
    #     self.assertTrue(changed)
    #     self.assertEqual(expected, schema)


    # def test_decimal_and_date(self):
    #     date_row = {"a_field": bson.timestamp.Timestamp(1565897157, 1)}
    #     decimal_row = {"a_field": bson.Decimal128(decimal.Decimal('1.34'))}

    #     schema = {"type": "object", "properties": {}}
                
    #     changed_date = common.row_to_schema_message(schema, date_row)
    #     changed_decimal = common.row_to_schema_message(schema, decimal_row)

    #     expected = {
    #         "type": "object",
    #         "properties": {
    #             "a_field": {
    #                 "anyOf": [
    #                     {"type": "string",
    #                      "format": "date-time"},
    #                     {"type": "number",
    #                      "multipleOf": 1e-34},
    #                     {}
    #                 ]
    #             }
    #         }
    #     }
    #     self.assertTrue(changed_date)
    #     self.assertTrue(changed_decimal)
    #     self.assertEqual(expected, schema)
        

    # def test_nested_data(self):
    #     date_row = {"foo": {"a_field": bson.timestamp.Timestamp(1565897157, 1)}}
    #     schema = {"type": "object", "properties": {}}
                
    #     changed = common.row_to_schema_message(schema, date_row)

    #     expected = {
    #         "type": "object",
    #         "properties": {
    #             "foo": {
    #                 "anyOf": [
    #                     {
    #                         "type": "object",
    #                         "properties": {
    #                             "a_field": {
    #                                 "anyOf": [
    #                                     {"type": "string",
    #                                      "format": "date-time"},
    #                                     {}
    #                                 ]
    #                             }
    #                         }
    #                     },
    #                     {}
    #                 ]
    #             }
    #         }
    #     }
    #     self.assertTrue(changed)
    #     self.assertEqual(expected, schema)

    # def test_date_and_nested_data(self):
    #     date_row = {"foo": bson.timestamp.Timestamp(1565897157, 1)}
    #     nested_row = {"foo": {"a_field": bson.timestamp.Timestamp(1565897157, 1)}}
    #     schema = {"type": "object", "properties": {}}
                
    #     changed_date = common.row_to_schema_message(schema, date_row)
    #     changed_nested = common.row_to_schema_message(schema, nested_row)

    #     expected = {
    #         "type": "object",
    #         "properties": {
    #             "foo": {
    #                 "anyOf": [
    #                     {
    #                         "type": "string",
    #                         "format": "date-time"
    #                     },
    #                     {
    #                         "type": "object",
    #                         "properties": {
    #                             "a_field": {
    #                                 "anyOf": [
    #                                     {"type": "string",
    #                                      "format": "date-time"},
    #                                     {}
    #                                 ]
    #                             }
    #                         }
    #                     },
    #                     {}
    #                 ]
    #             }
    #         }
    #     }
    #     self.assertTrue(changed_date)
    #     self.assertTrue(changed_nested)
    #     self.assertEqual(expected, schema)

    # def test_array_multiple_types(self):
    #     row = {
    #         "foo": [
    #             bson.timestamp.Timestamp(1565897157, 1),
    #             bson.Decimal128(decimal.Decimal('1.34'))
    #         ]
    #     }
    #     schema = {"type": "object", "properties": {}}
    #     changed = common.row_to_schema_message(schema, row)

    #     expected = {
    #         "type": "object",
    #         "properties": {
    #             "foo": {
    #                 "anyOf": [
    #                     {
    #                         "type": "array",
    #                         "items": {
    #                             "anyOf": [
    #                                 {
    #                                     "type": "string",
    #                                     "format": "date-time"
    #                                 },
    #                                 {
    #                                     "type": "number",
    #                                     "multipleOf": 1e-34
    #                                 },
    #                                 {}
    #                             ]
    #                         }
    #                     },
    #                     {}
    #                 ]
    #             }
    #         }
    #     }
    #     self.assertTrue(changed)
    #     self.assertEqual(expected, schema)
