"""
Provide hierarchy of exceptions for pycytominer-transform
"""


class CytominerException(Exception):
    """
    Root exception for custom hierarchy of exceptions
    with pycytominer-transform
    """


class NoInputDataException(CytominerException):
    """
    Exception for no input data
    """


class DatatypeException(CytominerException):
    """
    Exception for datatype challenges
    """


class SchemaException(CytominerException):
    """
    Exception for schema challenges
    """
