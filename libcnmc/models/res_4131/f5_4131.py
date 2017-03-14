from __future__ import absolute_import

from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict

from libcnmc.models.fields import String, Integer, Decimal


class F5Res4131(CNMCModel):
    """
        Class for fifth file of resolution 4771(Maquines)
    """
    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('denominacion', String()),
        ('codigo_ccuu', String()),
        ('codigo_ccaa', Integer()),
        ('tension_primario', Decimal(2)),
        ('tension_secundario', Decimal(2)),
        ('participacion', Decimal(2)),
        ('fecha_aps', String()),
        ('fecha_baja', String()),
        ('capacidad', Decimal(2)),
        ('estado', Integer())
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





