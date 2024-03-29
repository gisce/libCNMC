#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import get_name_ti, get_codigo_ccaa, format_f
from libcnmc.core import StopMultiprocessBased


class DES(StopMultiprocessBased):
    """
    Class to generate F6 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs:
        :type kwargs: dict
        """

        self.year = kwargs.pop("year")
        super(DES, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        id_resum = get_resum_any_id(self.connection, self.year)
        search_des = [("resums_inversio", "in", id_resum)]

        return self.connection.GiscedataCnmcDespatx.search(search_des)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "codi", "finalitat", "id_instalacio", "cini", "codigo_ccaa",
            "any_apm", "vol_total_inv", "ajudes", "vpi_retri", "estado",
            "actuacio_elegible_prtr"
        ]

        while True:
            try:
                item = self.input_q.get()
                if item == "STOP":
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                des = O.GiscedataCnmcDespatx.read(item, fields_to_read)
                output = [
                    des["codi"][1],
                    des["finalitat"],
                    des["id_instalacio"],
                    des["cini"],
                    get_codigo_ccaa(O, des["codigo_ccaa"][0]),
                    des["any_apm"],
                    format_f(des["vol_total_inv"], 2) or "0.00",
                    format_f(des["ajudes"], 2) or "0.00",
                    format_f(des["vpi_retri"], 2) or "0.00",
                    des["estado"],
                    des["actuacio_elegible_prtr"] or '',
                ]
                self.output_q.put(output)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
