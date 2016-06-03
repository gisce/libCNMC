from cnmcmodel import CNMCModel
from collections import OrderedDict

from .fields import String, Integer, Decimal


class F3Res4771(CNMCModel):
    """
        Model for third file of 4771 resolution(Subestacions)
    """
    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('denominacion', String()),
        ('codigo_ccaa', Integer()),
        ('participacion', Decimal(2)),
        ('fecha_aps', String()),
        ('posiciones', Integer())
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = ['fecha_aps']
        if self.diff(other, comp_fields):
            return True
        else:
            return False





