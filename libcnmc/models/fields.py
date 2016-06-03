# coding=utf-8
from UserDict import IterableUserDict


class DecimalCoerce(object):

    def __init__(self, decimals=1):
        self.deciamls = decimals * -1

    def __call__(self, value):
        from decimal import Decimal
        if isinstance(value, basestring):
            value = value.replace(',', '.')
        value = Decimal(value).quantize(Decimal(10) ** self.deciamls)
        return value


class Field(IterableUserDict):
    pass


class String(Field):
    def __init__(self):
        self.data = {'type': 'string'}


class Integer(Field):
    def __init__(self):
        self.data = {'type': 'integer', 'coerce': int}


class Decimal(Field):
    def __init__(self, precision):
        self.data = {'type': 'decimal', 'coerce': DecimalCoerce(precision)}