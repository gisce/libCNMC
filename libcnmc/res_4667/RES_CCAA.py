#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import format_f, get_codigo_ccaa
from libcnmc.core import StopMultiprocessBased


class RESCCAA(StopMultiprocessBased):
    """
    Class to generate F2 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """
        self.year = kwargs.pop("year")

        super(RESCCAA, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        model_ccaa = self.connection.GiscedataCnmcResum_ccaa

        ids_resums = get_resum_any_id(self.connection, self.year)
        search_res = [("resums_inversio", "in", ids_resums)]

        return model_ccaa.search(search_res)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line
        :rtype: str
        """

        fields_read = [
            "anio_periodo",
            "codigo_ccaa",
            "vol_total_inv_prv_ccaa",
            "ayudas_prv_ccaa",
            "financiacion_prv_ccaa",
            "vpi_retribuible_prv_ccaa",
            "vol_total_inv_bt_prv_ccaa",
            "num_proyectos_ccaa",
            "vol_total_inv_gr_prv_ccaa",
            "vol_total_inv_prv_ccaa_prtr",
            "ayudas_prv_ccaa_prtr",
            "financiacion_prv_ccaa_prtr",
            "vpi_retribuible_prv_ccaa_prtr",
            "num_proyectos_prtr",
        ]
        model_ccaa = self.connection.GiscedataCnmcResum_ccaa
        while True:
            try:
                item = self.input_q.get()
                if item == "STOP":
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                resum = model_ccaa.read(item, fields_read)

                output = [
                    get_codigo_ccaa(self.connection, resum["codigo_ccaa"][0]),
                    resum["anio_periodo"],
                    format_f(resum["vol_total_inv_prv_ccaa"], 2) or "0.00",
                    format_f(resum["ayudas_prv_ccaa"], 2) or "0.00",
                    format_f(resum["financiacion_prv_ccaa"], 2) or "0.00",
                    format_f(resum["vpi_retribuible_prv_ccaa"], 2) or "0.00",
                    resum["num_proyectos_ccaa"],
                    format_f(resum["vol_total_inv_bt_prv_ccaa"], 2) or "0.00",
                    format_f(resum["vol_total_inv_gr_prv_ccaa"], 2) or "0.00",
                    format_f(resum["vol_total_inv_prv_ccaa_prtr"], 2) or "0.00",
                    format_f(resum["ayudas_prv_ccaa_prtr"], 2) or "0.00",
                    format_f(resum["financiacion_prv_ccaa_prtr"], 2) or "0.00",
                    format_f(resum["vpi_retribuible_prv_ccaa_prtr"], 2) or "0.00",
                    resum["num_proyectos_prtr"] or "0",
                ]
                self.output_q.put(output)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
