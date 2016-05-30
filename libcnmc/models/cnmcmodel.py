from collections import namedtuple
import json


class CNMCModel(object):
    fields = [
        'id',
        'cini',
        'tipus_instalacio',
    ]

    @property
    def ref(self):
        return 1

    def __init__(self, *values, **kwvalues):
        stored = namedtuple('{0}_store'.format(self.__class__.__name__), self.fields)
        self.store = stored(*values, **kwvalues)

    def dump(self, out_format='json'):
        if out_format == 'json':
            return json.dumps(self.store._asdict())
        else:
            return list(self.store)

    def diff(self, obj, fields=None):
        assert isinstance(obj, self.__class__)
        diffs = {}
        if fields is None:
            fields = self.store._fields
        for field in fields:
            self_value = getattr(self, field)
            other_value = getattr(obj, field)
            if self_value != other_value:
                diffs[field] = (self_value, other_value)
        return diffs

    def __cmp__(self, other):
        raise NotImplementedError
