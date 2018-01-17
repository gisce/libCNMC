from __future__ import absolute_import

from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict

from libcnmc.models.fields import String, Integer, Decimal


class F5Res4666(CNMCModel):
    """
        Class for fifth file of resolution 4666(Maquines)
    """
    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('denominacion', String()),
        ('codigo_ccuu', String()),
        ('codigo_ccaa', Integer()),
        ('tension_primario', Decimal(3)),
        ('tension_secundario', Decimal(3)),
        ('participacion', Decimal(3)),
        ('fecha_aps', String()),
        ('fecha_baja', String()),
        ('capacidad', Decimal(3)),
        ('estado', Integer())
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = [
            'cini', 'codigo_ccuu', 'tension_primario', 'tension_secundario',
            'participacion', 'capacidad', 'denominacion'
        ]
        if self.diff(other, comp_fields):
            return True
        else:
            return False





