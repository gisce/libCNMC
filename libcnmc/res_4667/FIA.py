#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import format_f, get_codigo_ccaa, get_name_ti
from libcnmc.core import MultiprocessBased


class FIA(MultiprocessBased):
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
        super(FIA, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        id_resum = get_resum_any_id(self.connection, self.year)
        search_fia = [("resums_inversio", "in", id_resum)]

        return self.connection.GiscedataCnmcFiabilitat.search(search_fia)

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
            "vpi_retri", "estado", "actuacio_elegible_prtr"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                fia = O.GiscedataCnmcFiabilitat.read(item, fields_to_read)
                output = [
                    fia["codi"][1],
                    fia["finalitat"],
                    fia["id_instalacio"],
                    fia["cini"],
                    get_name_ti(O, fia["codi_tipus_inst"][0]),
                    get_codigo_ccaa(O, fia["ccaa"][0]),
                    fia["any_apm"],
                    format_f(fia["vol_total_inv"], 2) or "0.00",
                    format_f(fia["ajudes"], 2) or "0.00",
                    format_f(fia["inv_financiada"], 2) or "0.00",
                    format_f(fia["vpi_retri"], 2) or "0.00",
                    fia["estado"],
                    fia["actuacio_elegible_prtr"] or '',
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
