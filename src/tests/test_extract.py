"""Tests for Extractor class"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import unittest
from ETL.extract import Extractor

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


class TestExtractClass(unittest.TestCase):
    def setUp(self):
        self.test_files = {'test_valid_csv_extraction': os.path.join(ROOT_DIR, 'data', 'valid.csv'),
                           'test_valid_csv_in_txt_format': os.path.join(ROOT_DIR, 'data', 'valid.txt'),
                           'test_invalid_csv_missing_values': os.path.join(ROOT_DIR, 'data',
                                                                           'invalid_missing_values.csv'),
                           'test_invalid_data_format': os.path.join(ROOT_DIR, 'data', 'invalid_data_format.csv')}

    def test_valid_csv_extraction(self):
        file = self.test_files['test_valid_csv_extraction']
        with Extractor(file_name=file) as record_extractor:
            self.assertIsInstance(record_extractor, Extractor)
            for record in record_extractor:
                self.assertIsInstance(record, tuple)

    def test_valid_csv_in_txt_format(self):
        file = self.test_files['test_valid_csv_in_txt_format']
        with Extractor(file_name=file) as record_extractor:
            self.assertIsInstance(record_extractor, Extractor)
            for record in record_extractor:
                self.assertIsInstance(record, tuple)

    def test_invalid_csv_missing_values(self):
        file = self.test_files['test_invalid_csv_missing_values']
        with Extractor(file_name=file) as record_extractor:
            with self.assertRaises(TypeError):
                [record for record in record_extractor]

    def test_invalid_data_format(self):
        file = self.test_files['test_invalid_data_format']
        with self.assertRaises(ValueError):
            with Extractor(file_name=file) as f:
                pass
            pass


if __name__ == '__main__':
    unittest.main()
