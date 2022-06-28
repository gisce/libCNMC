#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import get_codigo_ccaa, get_name_ti, format_f


class LBT(MultiprocessBased):
    """
    Class to generate F2 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """
        self.year = kwargs.pop("year")
        super(LBT, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report
        
        :return: List of ids
        :rtype: list
        """
        id_resum = get_resum_any_id(self.connection, self.year)
        search_macro = [("resums_inversio", "in", id_resum)]

        return self.connection.GiscedataCnmcLiniesbt.search(search_macro)

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
            "financiacion_prv", "vpi_retribuible_prv", "estado",
            "actuacio_elegible_prtr"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                lbt = O.GiscedataCnmcLiniesbt.read(item, fields_to_read)
                output = [
                    lbt["cod_proyecto"][1],
                    lbt["finalidad"],
                    lbt["identificador_py"],
                    lbt["cini_prv"],
                    get_name_ti(O, lbt["cod_tipo_inst"][0]),
                    get_codigo_ccaa(O, lbt["codigo_ccaa_1"][0]),
                    get_codigo_ccaa(O, lbt["codigo_ccaa_2"][0]),
                    lbt["anio_prev_aps"],
                    format_f(lbt["longitud_prv"], 3) or "0.00",
                    lbt["capacidad_prv"],
                    format_f(lbt["vol_inv_prev"], 2) or "0.00",
                    format_f(lbt["ayudas_prv"], 2) or "0.00",
                    format_f(lbt["financiacion_prv"], 2) or "0.00",
                    format_f(lbt["vpi_retribuible_prv"], 2) or "0.00",
                    lbt["estado"],
                    lbt["actuacio_elegible_prtr"] or '',
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
