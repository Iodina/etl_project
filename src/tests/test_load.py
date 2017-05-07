"""Tests for Loader class are to be implemented"""
import os
import sys
import datetime
import unittest

import mongomock

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from ETL.load import Loader
from ETL.transform import Pitcher, Hitter

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT = os.path.join(ROOT_DIR, 'data', 'valid.csv')


class TestLoaderClass(unittest.TestCase):
    def setUp(self):
        self.loader = Loader()
        self.loader.db = mongomock.MongoClient(
            os.environ.get('DB_PORT_27017_TCP_ADDR',
                           'localhost'), 27017).db
        self.hitter = Hitter('aasedo01', 'Don', 'Aase', 190)
        self.pitcher = Pitcher('abadan01', 'Andy', 'Abad', 184)

    def tearDown(self):
        mongomock.MongoClient().drop_database(self.loader.db)

    def test_insert_hitter(self):
        self.loader.load(self.hitter)
        expected = {'nameFirst': 'Don', 'nameLast': 'Aase', 'weight': 190, '_id': 'aasedo01'}
        self.assertDictEqual(self.loader.db.hitter.find_one(), expected)

    def test_update_hitter(self):
        self.loader.load(self.hitter)
        self.loader.load(self.hitter)
        expected = {'nameFirst': 'Don', 'nameLast': 'Aase', 'weight': 190, '_id': 'aasedo01'}
        self.assertDictEqual(self.loader.db.hitter.find_one({'_id': self.hitter.playerID}), expected)

    def test_insert_pitcher(self):
        self.loader.load(self.pitcher)
        expected = {'nameFirst': 'Andy', 'nameLast': 'Abad', 'weight': 184, '_id': 'abadan01'}
        self.assertDictEqual(self.loader.db.pitcher.find_one(), expected)

    def test_update_weight_changed(self):
        self.loader.load(self.hitter)
        self.hitter = self.hitter._replace(weight=400)
        self.loader.load(self.hitter)
        expected = {'nameFirst': 'Don', 'nameLast': 'Aase', 'weight': 400, '_id': 'aasedo01'}
        self.assertDictEqual(self.loader.db.hitter.find_one({'_id': self.hitter.playerID}), expected)

    def test_update_weight_and_name_changed(self):
        self.loader.load(self.hitter)
        self.hitter = self.hitter._replace(weight=400, nameFirst='Bob')
        self.loader.load(self.hitter)
        expected = {'nameFirst': 'Don', 'nameLast': 'Aase', 'weight': 400, '_id': 'aasedo01'}
        self.assertDictEqual(self.loader.db.hitter.find_one({'_id': self.hitter.playerID}), expected)

    def test_save_error(self):
        try:
            raise ValueError('Some ValueError occurred')
        except ValueError as e:
            self.loader.save_error(e)
        expected = {'type': str(ValueError), 'date': datetime.datetime.now().timestamp()}
        self.assertEqual(self.loader.db.errors.find_one()['type'], expected['type'])
        date_time_date = datetime.datetime.strptime(self.loader.db.errors.find_one()['date'], "%Y-%m-%d %H:%M:%S.%f")
        self.assertAlmostEqual(date_time_date.timestamp(), expected['date'], delta=0.001)


if __name__ == '__main__':
    unittest.main()
