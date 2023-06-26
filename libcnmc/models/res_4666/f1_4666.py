from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict
from libcnmc.models.fields import String, Integer, Decimal
import string
import re


class F1Res4666(CNMCModel):
    """
        Class for second file of resolution 4666(LAT) (Trams AT)
    """
    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('origen', String()),
        ('destino', String()),
        ('codigo_ccuu', String()),
        ('codigo_ccaa_1', Integer()),
        ('codigo_ccaa_2', Integer()),
        ('participacion', Decimal(2)),
        ('fecha_aps', String()),
        ('fecha_baja', String()),
        ('numero_circuitos', Integer()),
        ('numero_conductores', Integer()),
        ('nivel_tension', Decimal(3)),
        ('longitud', Decimal(3)),
        ('intensidad_maxima', Decimal(3)),
        ('seccion', Decimal(3)),
        ('capacidad', Integer()),
        ('estado', Integer())
        ])

    def ref_or_regulatori(self, trams_at_prefix, trams_bt_prefix):
        return re.sub('^{}'.format(trams_at_prefix), '', self.store.identificador)

    def __cmp__(self, other):
        comp_fields = [
            'longitud', 'cini', 'codigo_ccuu', 'nivel_tension', 'fecha_aps'
        ]
        if self.diff(other, comp_fields):
            return True
        else:
            return False





