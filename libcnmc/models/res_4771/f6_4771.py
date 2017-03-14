from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict

from libcnmc.models.fields import String, Integer

class F6Res4771(CNMCModel):
    """
        Class for sixth file of resolution 4771(Despatxos)
    """

    schema = OrderedDict([
        ('identificador', String()),
        ('cini', String()),
        ('denominacion', String()),
        ('anio_aps', String()),
        ('valor_inversion', Integer())
    ])

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = []
        if self.diff(other, comp_fields):
            return True
        else:
            return False





