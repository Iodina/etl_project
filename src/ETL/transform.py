"""Class Transformer used for transforming csv row."""
from collections import namedtuple

from ETL.exceptions import EmptyValueException
from config import REQUIRED_FIELDS, TABLE_RESOLVER_FIELD

DEST_METADATA = ('playerID', 'nameFirst', 'nameLast', 'weight')

Pitcher = namedtuple('Pitcher', DEST_METADATA)
Hitter = namedtuple('Hitter', DEST_METADATA)


class Transformer(object):
    """Class Transformer.

    Transform data according to the rules:
    1.If required fields values are empty ->
        raise raise ETL.exceptions.EmptyValueException
    2.If `position` field == 'Pitcher' ->
        return ETL.transform.Pitcher object
    3.If `position` field != 'Pitcher' ->
        return ETL.transform.Hitter object
    """

    def __init__(self, required_data=REQUIRED_FIELDS):
        """Instantiate Transformer.

        :param required_data: list of required field names
        """
        self.required = required_data

    def filter_required_fields(self, data):
        """Filter data by required fields.

        :param data: ETL.extract.PlayerRecord instance
        :return: dict with filtered values
        """
        filtered_data = {name: value for req in self.required
                         for name, value in data._asdict().items()
                         if req == name}
        return filtered_data

    def filter_nonempty_fields(self, data):
        """Filter filtered by required data by nonempty fields.

        :param data: ETL.extract.PlayerRecord instance
        :return: dict with filtered values
        :raises ETL.exceptions.EmptyValueException:
            required fields values are empty.
        """
        filtered = self.filter_required_fields(data)
        dict_with_empty_values = {name: value
                                  for name, value in filtered.items()
                                  if not value}
        if dict_with_empty_values:
            raise EmptyValueException(str(data._asdict()),
                                      str(dict_with_empty_values.keys()))
        return filtered

    def is_player_pitcher(self, filtered_required_nonempty):
        """Decide whether input data is a Pitcher or non-Pitcher"""
        return filtered_required_nonempty.get(TABLE_RESOLVER_FIELD) == 'Pitcher'

    def transform(self, data):
        """Transform data according to the rules.

        Rules for transforming:
            1.If required fields values are empty ->
                raise raise ETL.exceptions.EmptyValueException
            2.If `position` field == 'Pitcher' ->
                return ETL.transform.Pitcher object
            3.If `position` field != 'Pitcher' ->
                return ETL.transform.Hitter object

        :param data: ETL.extract.PlayerRecord object
        :return transformed data: ETL.transform.Pitcher object or ETL.transform.Hitter object
        """
        filter_req_nonempty = self.filter_nonempty_fields(data)
        if self.is_player_pitcher(filter_req_nonempty):
            filter_req_nonempty.pop(TABLE_RESOLVER_FIELD)
            return Pitcher(**filter_req_nonempty)
        else:
            filter_req_nonempty.pop(TABLE_RESOLVER_FIELD)
            return Hitter(**filter_req_nonempty)
