#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import format_f, get_name_ti, get_codigo_ccaa
from libcnmc.core import MultiprocessBased


class CT(MultiprocessBased):
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
        super(CT, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        id_resum = get_resum_any_id(self.connection, self.year)
        search_ct = [("resums_inversio", "in", id_resum)]

        return self.connection.GiscedataCnmcCt.search(search_ct)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "codi", "finalitat", "id_instalacio", "cini", "codi_tipus_inst",
            "ccaa", "any_apm", "vol_total_inv", "ajudes", "inv_financiada",
            "vpi_retri", "estado"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                ct = O.GiscedataCnmcCt.read(item, fields_to_read)
                output = [
                    ct["codi"][1],
                    ct["finalitat"],
                    ct["id_instalacio"],
                    ct["cini"],
                    get_name_ti(O, ct["codi_tipus_inst"][0]),
                    get_codigo_ccaa(O, ct["ccaa"][0]),
                    ct["any_apm"],
                    format_f(ct["vol_total_inv"], 3),
                    format_f(ct["ajudes"], 3),
                    format_f(ct["inv_financiada"], 3),
                    format_f(ct["vpi_retri"], 3),
                    ct["estado"]
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
