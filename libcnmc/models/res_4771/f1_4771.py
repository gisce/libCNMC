from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict

from libcnmc.models.fields import String, Integer, Decimal


class F1Res4771(CNMCModel):
    """
        Class for second file of resolution 4771(LAT)
    """
    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('origen', String()),
        ('destino', String()),
        ('codigo_tipo_linea', String()),
        ('codigo_ccaa_1', Integer()),
        ('codigo_ccaa_2', Integer()),
        ('participacion', Decimal(2)),
        ('fecha_aps', String()),
        ('numero_circuitos', Integer()),
        ('numero_conductores', Integer()),
        ('nivel_tension', Decimal(2)),
        ('longitud', Decimal(3)),
        ('intensidad_maxima', Decimal(2)),
        ('seccion', Decimal(2)),
        ('capacidad', Integer()),
        ('propiedad', Integer())
        ])

    @property
    def ref(self):
        return self.store.identificador[1:]

    def __cmp__(self, other):
        comp_fields = [
            'longitud', 'cini', 'seccion', 'capacidad'
        ]
        if self.diff(other, comp_fields):
            return True
        else:
            return False





