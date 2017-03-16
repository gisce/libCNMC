from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict

from libcnmc.models.fields import String, Integer, Decimal


class F8Res4131(CNMCModel):
    """
        Class for eight file of resolution 4131(CT)
    """

    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('denominacion', String()),
        ('codigo_ccuu', String()),
        ('codigo_ccaa', Integer()),
        ('participacion', Decimal(3)),
        ('fecha_aps', String()),
        ('fecha_baja', String()),
        ('estado', Integer())
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = ['cini']
        if self.diff(other, comp_fields):
            return True
        else:
            return False





