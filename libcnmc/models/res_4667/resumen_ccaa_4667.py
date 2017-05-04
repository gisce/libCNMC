from __future__ import absolute_import
from libcnmc.models.cnmcmodel import CNMCModel
from collections import OrderedDict
from libcnmc.models.fields import String, Integer, Decimal


class ResumenCCAARes4667(CNMCModel):
    """
    Class for Resumen file of 4667 
    """

    schema = OrderedDict([
        ('anio_periodo', String()),
        ('limite_empresa', Decimal(12)),
        ('demanda_empresa_p0', Decimal(8)),
        ('inc_demanda_empresa_prv', Decimal(8)),
        ('frri', Decimal(8)),
        ('vpi_superado_prv', String()),
        ('vol_total_inv_prv', Decimal(12)),
        ('ayudas_prv', Decimal(12)),
        ('financiacion_prv', Decimal(8)),
        ('vpi_retribuible_prv', Decimal(10)),
        ('num_proyectos', Integer()),
        ('vol_total_inv_bt_prv', Decimal(12)),
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
