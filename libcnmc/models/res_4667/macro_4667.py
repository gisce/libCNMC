from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict
from libcnmc.models.fields import String, Integer, Decimal


class MacroRes4667(CNMCModel):
    """
    Class for Macroeconomic file of 4667 
    """

    schema = OrderedDict([
        ('anio_periodo', String()),
        ('crec_pib', Decimal(6)),
        ('pib_prev', Decimal(10)),
        ('limites_sector', Decimal(8)),
        ('inc_demanda_sector', Decimal(6)),
    ])

    @property
    def ref(self):
        """
        Returns the identifier
        :return: Identifier 
        :rtype: str
        """

        return self.store.identificador_py

    def __cmp__(self, other):
        """
        Overwrites the comparison operator

        :param other: Element to compare
        :return:
        :rtype: str
        """

        comp_fields = []
        if self.diff(other, comp_fields):
            return True
        else:
            return False
