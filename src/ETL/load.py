"""Class Loader used for loading data to MongoDB. """

import os
import datetime

from pymongo import MongoClient
from config import UPDATABLE_FIELDS, ID_FIELD, \
    REQUIRED_FIELDS, TABLE_RESOLVER_FIELD


class Loader(object):
    """Load valid data or errors to database

    When instantiating Loader connection to db is established.
    """

    def __init__(self):
        client = MongoClient(os.environ.get('DB_PORT_27017_TCP_ADDR',
                                            'localhost'), 27017)
        self.db = client.target_database

    def load(self, data):
        """Load data to db.

        If document with same ID exists, field weight is updated.
        Otherwise, new document is added to the pitcher or hitter collection,
        depending on `data`.

        :param data: object of type ETL.transform.Hitter OR ETL.transform.Pitcher
        """
        table_name = type(data).__name__.lower()
        document = dict(data._asdict())
        document_updatable_part = {field: document[field]
                                   for field in UPDATABLE_FIELDS}
        document_remain_part = {field: document[field]
                                for field in REQUIRED_FIELDS
                                if field not in UPDATABLE_FIELDS +
                                (ID_FIELD,) +
                                (TABLE_RESOLVER_FIELD,)}
        self.db[table_name].update_one(
            {"_id": document.get(ID_FIELD)},
            {"$set": document_updatable_part,
             "$setOnInsert": document_remain_part},
            upsert=True
        )

    def save_error(self, e):
        """Load error data to db.

        Add type and date information.

        :param e: exception object
        """
        err = e.__dict__
        err['date'] = str(datetime.datetime.now())
        err['type'] = str(type(e))
        self.db.errors.insert_one(err)
