from collections import namedtuple, OrderedDict
import json
from decimal import Decimal, InvalidOperation

from cerberus import Validator
from cerberus.errors import ERROR_BAD_TYPE


class CNMCValidator(Validator):
    """CNMC Schema validator

    - Add support for decimal type
    """
    def _validate_type_decimal(self, field, value):
        if not isinstance(value, Decimal):
            try:
                Decimal(value)
            except InvalidOperation:
                self._error(field, ERROR_BAD_TYPE.format('Decimal'))


def json_decimal_default(o):
    """
    Let export decimal values in JSON
    :param o: value to export
    :return: value converted to string
    """
    if isinstance(o, Decimal):
        return str(o)


class CNMCModel(object):
    """
    CNMC Model base
    """

    schema = OrderedDict([
        ('id', {'type': 'string'}),
        ('cini', {'type': 'string'}),
        ('tipus_instalacio', {'type': 'string'})
    ])

    @property
    def fields(self):
        return self.schema.keys()

    @property
    def ref(self):
        return 1

    def __init__(self, *values, **kwvalues):
        self.validator = CNMCValidator(self.schema)
        stored = namedtuple('{0}_store'.format(self.__class__.__name__), self.fields)
        self.store = stored(*values, **kwvalues)
        self.validator.validate(self.store._asdict())
        self.store = stored(**self.validator.document)

    def dump(self, out_format='json'):
        if out_format == 'json':
            return json.dumps(self.store._asdict(), default=json_decimal_default, ensure_ascii=False)
        else:
            return list(self.store)

    def diff(self, obj, fields=None):
        assert isinstance(obj, self.__class__)
        diffs = {}
        if fields is None:
            fields = self.store._fields
        for field in fields:
            self_value = getattr(self.store, field)
            other_value = getattr(obj.store, field)
            if self_value != other_value:
                diffs[field] = (self_value, other_value)
        return diffs

    def __cmp__(self, other):
        raise NotImplementedError
