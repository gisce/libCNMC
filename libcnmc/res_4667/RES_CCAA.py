#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.core import MultiprocessBased


class RESCCAA(MultiprocessBased):
    """
    Class to generate F2 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(RESCCAA, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        search_params = []
        return self.connection.GiscedataCnmcResumCCAA.search(search_params)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "codigo_ccaa", "anio_periodo", "vol_total_inv_prv_ccaa",
            "ayudas_prv_ccaa", "financiacion_prv_ccaa",
            "vpi_retribuible_prv_ccaa", "num_proyectos_ccaa",
            "vol_total_inv_bt_prv_ccaa"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                res_ccaa = O.GiscedataCnmcResumCCAA.read(item, fields_to_read)
                output = [
                    res_ccaa["codigo_ccaa"],
                    res_ccaa["anio_periodo"],
                    res_ccaa["vol_total_inv_prv_ccaa"],
                    res_ccaa["ayudas_prv_ccaa"],
                    res_ccaa["financiacion_prv_ccaa"],
                    res_ccaa["vpi_retribuible_prv_ccaa"],
                    res_ccaa["num_proyectos_ccaa"],
                    res_ccaa["vol_total_inv_bt_prv_ccaa"],
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
