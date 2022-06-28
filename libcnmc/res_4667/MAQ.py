#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import format_f, get_name_ti, get_codigo_ccaa
from libcnmc.core import MultiprocessBased


class MAQ(MultiprocessBased):
    """
    Class to generate F5 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs:
        :type kwargs: dict
        """

        self.year = kwargs.pop("year")
        super(MAQ, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        id_resum = get_resum_any_id(self.connection, self.year)
        search_maq = [("resums_inversio", "in", id_resum)]

        return self.connection.GiscedataCnmcMaquines.search(search_maq)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "codi", "finalitat", "id_instalacio", "cini", "codi_tipus_inst",
            "ccaa", "any_apm", "pot_inst_prev", "vol_total_inv", "ajudes",
            "inv_financiada", "vpi_retri", "estado", "actuacio_elegible_prtr"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                maq = O.GiscedataCnmcMaquines.read(item, fields_to_read)
                output = [
                    maq["codi"][0],
                    maq["finalitat"],
                    maq["id_instalacio"],
                    maq["cini"],
                    get_name_ti(O, maq["codi_tipus_inst"][0]),
                    get_codigo_ccaa(O, maq["ccaa"][0]),
                    maq["any_apm"],
                    format_f(maq["pot_inst_prev"], 3),
                    format_f(maq["vol_total_inv"], 2) or "0.00",
                    format_f(maq["ajudes"], 2) or "0.00",
                    format_f(maq["inv_financiada"], 2) or "0.00",
                    format_f(maq["vpi_retri"], 2) or "0.00",
                    maq["estado"],
                    maq["actuacio_elegible_prtr"] or '',
                ]
                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
