#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.res_4667.utils import get_resum_any_id


class LBT(MultiprocessBased):
    """
    Class to generate F2 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(LBT, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report
        
        :return: List of ids
        :rtype: list
        """
        id_resum = get_resum_any_id(self.connection, self.year)
        search_macro = [("resums_inversio", "=", id_resum)]

        return self.connection.GiscedataCnmcLiniesBT.search(search_macro)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "cod_proyecto", "finalidad", "identificador_py", "cini_prv",
            "cod_tipo_inst", "codigo_ccaa_1", "codigo_ccaa_2", "anio_prev_aps",
            "longitud_prv", "capacidad_prv", "vol_inv_prev", "ayudas_prv",
            "financiacion_prv", "vpi_retribuible_prv", "estado"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                lbt = O.GiscedataCnmcLiniesBT.read(item, fields_to_read)
                output = [
                    lbt["cod_proyecto"],
                    lbt["finalidad"],
                    lbt["identificador_py"],
                    lbt["cini_prv"],
                    lbt["cod_tipo_inst"],
                    lbt["codigo_ccaa_1"],
                    lbt["codigo_ccaa_2"],
                    lbt["anio_prev_aps"],
                    lbt["longitud_prv"],
                    lbt["capacidad_prv"],
                    lbt["vol_inv_prev"],
                    lbt["ayudas_prv"],
                    lbt["financiacion_prv"],
                    lbt["vpi_retribuible_prv"],
                    lbt["estado"]
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
