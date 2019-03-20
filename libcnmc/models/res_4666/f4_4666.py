from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel

from collections import OrderedDict

from libcnmc.models.fields import String, Integer, Decimal


class F4Res4666(CNMCModel):
    """
        Class for forth file of resolution 4666(Posiciones)
    """
    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('denominacion', String()),
        ('codigo_ccuu', String()),
        ('codigo_ccaa', Integer()),
        ('nivel_tension', Decimal(3)),
        ('participacion', Decimal(3)),
        ('fecha_aps', String()),
        ('fecha_baja', String()),
        ('estado', Integer()),
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = [
            'cini', 'codigo_ccuu', 'nivel_tension', 'participacion',
            'denominacion', 'fecha_aps'
        ]
        if self.diff(other, comp_fields):
            return True
        else:
            return False





