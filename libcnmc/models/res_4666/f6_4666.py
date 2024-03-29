from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel

from collections import OrderedDict

from libcnmc.models.fields import String, Integer, Decimal


class F6Res4666(CNMCModel):
    """
        Class for sixth file of resolution 4666(Despatxos)
    """

    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('denominacion', String()),
        ('fecha_aps', String()),
        ('fecha_baja', String()),
        ('valor_inversion', Decimal(3)),
        ('estado', Integer())
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = ['cini', 'fecha_aps']
        if self.diff(other, comp_fields):
            return True
        else:
            return False





