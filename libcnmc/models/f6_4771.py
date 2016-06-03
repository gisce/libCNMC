from cnmcmodel import CNMCModel
from collections import OrderedDict

from .fields import String, Integer

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
        # TODO: Add comparsion fields
        comp_fields = []
        if self.diff(other, comp_fields):
            return True
        else:
            return False





