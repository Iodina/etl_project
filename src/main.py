"""This module performs ETL process: data extraction, transformation and loading."""
import csv
import os
import sys

from pymongo.errors import PyMongoError

from ETL.exceptions import EtlTransformException, MalformedCsvError
from ETL.extract import Extractor
from ETL.load import Loader
from ETL.transform import Transformer
from config import REQUIRED_FIELDS, INPUT_FILE

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":
    # Handle command line arguments if any
    if len(sys.argv) > 1:
        input_files = []
        for i in sys.argv[1:]:
            input_files.append(os.path.abspath(i))
    else:
        input_files = [os.path.join(ROOT_DIR, '..', 'input', INPUT_FILE)]

    # For each input file instantiate Loader, Transformer and Extractor
    # And then run ETL process
    for file in input_files:
        if os.path.exists(file):
            record_loader = Loader()
            record_transformer = Transformer(required_data=REQUIRED_FIELDS)
            try:
                with Extractor(file_name=file) as record_extractor:
                    # ETL process
                    for record in record_extractor:
                        try:
                            record_loader.load(record_transformer.transform(record))
                        except (EtlTransformException, PyMongoError) as e:
                            record_loader.save_error(e)
            except (TypeError, ValueError, csv.Error) as e:
                raise MalformedCsvError('Error occurred during parsing the csv document', e) from None
        else:
            raise FileNotFoundError('There is no such file', file)
