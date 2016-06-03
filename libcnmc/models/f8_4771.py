from cnmcmodel import CNMCModel
from collections import OrderedDict

from .fields import String, Integer, Decimal


class F8Res4771(CNMCModel):
    """
        Class for eight file of resolution 4771(CT)
    """

    schema = OrderedDict([
        {'identificador': String()},
        {'cini': String()},
        {'denominacion': String()},
        {'codigo_tipo_ct': String()},
        {'codigo_ccaa': Integer()},
        {'participacion': Decimal(2)},
        {'fecha_aps': String()}
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = ['cini', 'fecha_aps']
        if self.diff(other, comp_fields):
            return True
        else:
            return False





