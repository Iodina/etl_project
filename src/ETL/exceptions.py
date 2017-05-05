"""Exceptions raised by ETL."""


class EtlError(Exception):
    """Base class for all ETL exceptions."""


class EtlTransformException(EtlError):
    """Base class for all ETL transform exceptions.

    Attributes:
        record -- input record in which the error occurred
        fields -- input fields in which the error occurred
    """

    def __init__(self, record, fields):
        self.record = record
        self.fields = fields


class EmptyValueException(EtlTransformException):
    """Exception raised for empty value in required parameters.

    Attributes:
        record -- input record in which the error occurred
        fields -- input fields in which the error occurred
        message -- information about exception
    """

    def __init__(self, record, fields):
        super().__init__(record, fields)
        self.message = 'Record %s has empty values in fields: %s' % (str(record), str(fields))


class MalformedCsvError(EtlError):
    """Exception raised for invalid input csv.

    Attributes:
        message -- message passed
        error -- exception object
    """

    def __init__(self, message, error):
        self.message = message
        self.error = error
