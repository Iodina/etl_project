"""Tests for Transformer class"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import unittest

from ETL.extract import Extractor
from ETL.transform import Transformer, Pitcher, Hitter
from ETL.exceptions import EmptyValueException

from config import REQUIRED_FIELDS, INPUT_FILE

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT = os.path.join(ROOT_DIR, 'data', 'valid.csv')

class TestTransformClass(unittest.TestCase):
    def setUp(self):
        with Extractor(file_name=INPUT) as self.record_extractor:
            self.record_transformer = Transformer(required_data=REQUIRED_FIELDS)
            self.test_record = {
                'test_filter_required_fields': next(self.record_extractor),
                'test_filter_nonempty_fields_success': next(self.record_extractor),
                'test_filter_nonempty_fields_fail': next(self.record_extractor),
                'test_transform_to_hitter': next(self.record_extractor),
                'test_transform_to_pitcher': next(self.record_extractor)
            }

    def test_filter_required_fields(self):
        data = self.test_record['test_filter_required_fields']
        expected = {"playerID": "aardsda01",
                    "nameFirst": "David",
                    "nameLast": "Aardsma",
                    "weight": "215",
                    "position": "CF"}
        self.assertDictEqual(self.record_transformer.filter_required_fields(data),
                             expected)

    def test_filter_nonempty_fields_success(self):
        data = self.test_record['test_filter_nonempty_fields_success']
        expected = {"playerID": "aaronha01",
                    "nameFirst": "Hank",
                    "nameLast": "Aaron",
                    "weight": "180",
                    "position": "RF"}
        self.assertDictEqual(self.record_transformer.filter_nonempty_fields(data),
                             expected)

    def test_filter_nonempty_fields_fail(self):
        data = self.test_record['test_filter_nonempty_fields_fail']
        with self.assertRaises(EmptyValueException):
            self.record_transformer.filter_nonempty_fields(data)

    def test_transform_to_pitcher(self):
        data = self.test_record['test_transform_to_pitcher']
        self.assertIsInstance(self.record_transformer.transform(data), Pitcher)

    def test_transform_to_hitter(self):
        data = self.test_record['test_transform_to_hitter']
        self.assertIsInstance(self.record_transformer.transform(data), Hitter)


if __name__ == '__main__':
    unittest.main()
