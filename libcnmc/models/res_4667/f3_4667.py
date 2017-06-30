from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict
from libcnmc.models.fields import String, Integer, Decimal


class F3Res4667(CNMCModel):
    """
    Class for Otros file of 4667 
    """

    schema = OrderedDict([
        ('cod_proyecto', String()),
        ('finalidad', String()),
        ('identificador_py', String()),
        ('cini_prv', String()),
        ('codigo_ccaa', Integer()),
        ('anio_prev_es', Integer()),
        ('vol_inv_prev', Decimal(10)),
        ('ayudas_prv', Decimal(10)),
        ('financiacion_prv', Decimal(8)),
        ('vpi_retribuible_prv', Decimal(10)),
        ('estado', Integer())
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
