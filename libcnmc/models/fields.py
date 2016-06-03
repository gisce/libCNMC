# coding=utf-8
from UserDict import IterableUserDict


class DecimalCoerce(object):
    """
    Coercing class to convert to decimals
    :param decimals: precision to get the decimal
    """

    def __init__(self, decimals=1):
        self.deciamls = decimals * -1

    def __call__(self, value):
        from decimal import Decimal
        if isinstance(value, basestring):
            value = value.replace(',', '.')
        value = Decimal(value).quantize(Decimal(10) ** self.deciamls)
        return value


class Field(IterableUserDict):
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