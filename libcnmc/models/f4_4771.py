from cnmcmodel import CNMCModel
from collections import OrderedDict

from .fields import String, Integer, Decimal


class F4Res4771(CNMCModel):
    """
        Class for forth file of resolution 4771(Posiciones)
    """
    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('denominacion', String()),
        ('codigo_tipo_posicion', String()),
        ('codigo_ccaa', Integer()),
        ('nivel_tension', Decimal(2)),
        ('participacion', Decimal(2)),
        ('fecha_aps', String()),
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = [
            'cini', 'nivel_tension', 'participacion',
            'denominacion', 'fecha_aps'
        ]
        if self.diff(other, comp_fields):
            return True
        else:
            return False





