from __future__ import absolute_import
from collections import OrderedDict

from libcnmc.models.cnmcmodel import CNMCModel
from libcnmc.models.fields import String, Integer, Decimal
import string


class F2Res4666(CNMCModel):
    """
        Class for second file of resolution 4666(BT) (Trams BT)
    """

    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('origen', String()),
        ('destino', String()),
        ('codigo_ccuu', String()),
        ('codigo_ccaa_1', Integer()),
        ('codigo_ccaa_2', Integer()),
        ('participacion', String()),
        ('fecha_aps', String()),
        ('fecha_baja', String()),
        ('numero_circuitos', Integer()),
        ('numero_conductores', Integer()),
        ('nivel_tension', Decimal(3)),
        ('longitud', Decimal(3)),
        ('intensidad_maxima', Decimal(3)),
        ('seccion', Decimal(3)),
        ('capacidad', Decimal(3)),
        ('estado', Integer())
    ])

    @property
    def ref(self):
        return ''.join(
            c for c in self.store.identificador if c in string.digits
        )

    def __cmp__(self, other):
        comp_fields = [
            'longitud', 'cini', 'seccion', 'codigo_ccuu', 'nivel_tension',
            'fecha_aps'
        ]
        if self.diff(other, comp_fields):
            return True
        else:
            return False





