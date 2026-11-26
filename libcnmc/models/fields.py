# coding=utf-8
try:
    from collections import UserDict
except ImportError:
    from UserDict import IterableUserDict as UserDict

try:
    string_type = basestring
except NameError:
    string_type = str


class DecimalCoerce(object):
    """
    Coercing class to convert to decimals
    :param decimals: precision to get the decimal
    """

    def __init__(self, decimals=3):
        self.deciamls = decimals * -1

    def __call__(self, value):
        from decimal import Decimal
        if isinstance(value, string_type):
            value = value.replace(',', '.')
        value = Decimal(value).quantize(Decimal(10) ** self.deciamls)
        return value


class Field(UserDict):
    """
    Base Field class
    """
    pass


class String(Field):
    """
    String field

    Hardcoded `{'type': 'string'}`
    """
    def __init__(self):
        self.data = {'type': 'string'}


class Integer(Field):
    """
    Integer field

    Hardcoded `{'type': 'integer', 'coerce': int}`
    """
    def __init__(self):
        self.data = {'type': 'integer', 'coerce': int}


class Decimal(Field):
    """
    Decimal field

    :param precision: Precision to use with decimal class

    Hardcoded `{'type': 'decimal', 'coerce': DecimalCoerce(precision)}`
    """
    def __init__(self, precision):
        self.data = {'type': 'decimal', 'coerce': DecimalCoerce(precision)}
