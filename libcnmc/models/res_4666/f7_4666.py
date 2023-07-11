from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict

from libcnmc.models.fields import String, Integer


class F7Res4666(CNMCModel):
    """
        Class for seventh file of resolution 4666(Fiabilidad)
    """

    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('elemento_act', String()),
        ('codigo_ccuu', String()),
        ('codigo_ccaa', Integer()),
        ('fecha_aps', String()),
        ('fecha_baja', String()),
        ('estado', Integer())
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = ['cini', 'codigo_ccuu', 'fecha_aps']
        if self.diff(other, comp_fields):
            return True
        else:
            return False





