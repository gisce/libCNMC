from __future__ import absolute_import
from collections import OrderedDict

from .cnmcmodel import CNMCModel
from .fields import String, Integer, Decimal


class F2Res4771(CNMCModel):
    """
        Class for second file of resolution 4771(BT)
    """

    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('origen', String()),
        ('destino', String()),
        ('codigo_tipo_linea', String()),
        ('codigo_ccaa_1', Integer()),
        ('codigo_ccaa_2', Integer()),
        ('participacion', String()),
        ('fecha_aps', String()),
        ('numero_circuitos', Integer()),
        ('numero_conductores', Integer()),
        ('nivel_tension', Decimal(2)),
        ('longitud', Decimal(3)),
        ('intensidad_maxima', Decimal(2)),
        ('seccion', Decimal(2)),
        ('capacidad', Decimal(2)),
        ('propiedad', String())
    ])

    @property
    def ref(self):
        return self.store.identificador[1:]

    def __cmp__(self, other):
        comp_fields = ['longitud', 'cini', 'seccion', 'capacidad', 'fecha_aps']
        if self.diff(other, comp_fields):
            return True
        else:
            return False





