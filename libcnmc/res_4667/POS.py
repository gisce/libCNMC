#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import format_f, get_name_ti, get_codigo_ccaa
from libcnmc.core import MultiprocessBased


class POS(MultiprocessBased):
    """
    Class to generate F5 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        self.year = kwargs.pop("year")
        super(POS, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        id_resum = get_resum_any_id(self.connection, self.year)
        search_pos = [("resums_inversio", "in", id_resum)]

        return self.connection.GiscedataCnmcPosicions.search(search_pos)

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

                pos = O.GiscedataCnmcPosicions.read(item, fields_to_read)
                output = [
                    pos["codi"][1],
                    pos["finalitat"],
                    pos["id_instalacio"],
                    pos["cini"],
                    get_name_ti(O, pos["codi_tipus_inst"][0]),
                    get_codigo_ccaa(O, pos["ccaa"][0]),
                    pos["any_apm"],
                    format_f(pos["vol_total_inv"], 2),
                    format_f(pos["ajudes"], 2),
                    format_f(pos["inv_financiada"], 2),
                    format_f(pos["vpi_retri"], 2),
                    pos["estado"]
                ]
                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
