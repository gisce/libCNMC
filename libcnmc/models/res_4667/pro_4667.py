from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict
from libcnmc.models.fields import String, Integer, Decimal


class ProyectoRes4667(CNMCModel):
    """
    Class for Poyecto file of 4667 
    """

    schema = OrderedDict([
        ('cod_proyecto', String()),
        ('nombre', String()),
        ('codigo_ccaa_1', Integer()),
        ('codigo_ccaa_2', Integer()),
        ('memo_descriptiva', String()),
        ('vol_total_inv_prev_proy', Decimal(10)),
        ('ayudas_prv_proy', Decimal(10)),
        ('financiacion_prev_proy', Decimal(8)),
        ('vpi_retribuible_prv', Decimal(12)),
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
