from cnmcmodel import CNMCModel
from collections import OrderedDict

from .fields import String, Integer


class F7Res4771(CNMCModel):
    """
        Class for seventh file of resolution 4771(Fiabilidad)
    """

    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('elemento_act', String()),
        ('codigo_tipo_inst', String()),
        ('codigo_ccaa', Integer()),
        ('fecha_aps', String())
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = ['cini', 'elemento_act']
        if self.diff(other, comp_fields):
            return True
        else:
            return False





