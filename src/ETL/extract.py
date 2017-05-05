"""Class Extractor used for csv data extraction."""
import csv

from collections import namedtuple


class Extractor(object):
    """Extract csv data.

    Extractor instance is iterator object that is used to iterate over
    csv document rows transformed into namedtuple
    """

    def __init__(self, file_name):
        """Instantiate Extractor

        Read and parses header (fields name) and create namedtuple.
        Create iterator over namedtuple instances.
        :param file_name: absolute path to file with data
        """
        try:
            self.open_file = open(file_name, "rt")
            reader = csv.reader(self.open_file)
            table_metadata = next(reader)
            PlayerRecord = namedtuple('PlayerRecord', table_metadata)
            self._iterator = map(PlayerRecord._make, reader)
        except Exception as e:
            if self.open_file:
                self.open_file.close()
            raise e

    def __iter__(self):
        return self

    def __next__(self):
        return self._iterator.__next__()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.open_file.close()