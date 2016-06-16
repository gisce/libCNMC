from cnmcmodel import CNMCModel
from collections import OrderedDict

from .fields import String, Integer, Decimal


class F5Res4771(CNMCModel):
    """
        Class for fifth file of resolution 4771(Maquines)
    """
    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('denominacion', String()),
        ('codigo_tipo_maquina', String()),
        ('codigo_ccaa', Integer()),
        ('tension_primario', Decimal(2)),
        ('tension_secundario', Decimal(2)),
        ('participacion', Decimal(2)),
        ('fecha_aps', String()),
        ('capacidad', Decimal(2))
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = [
            'cini', 'tension_primario', 'tension_secundario',
            'capacidad', 'denominacion',
        ]
        if self.diff(other, comp_fields):
            return True
        else:
            return False





