#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.core import MultiprocessBased


class FIA(MultiprocessBased):
    """
    Class to generate F6 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(FIA, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        id_resum = get_resum_any_id(self.connection, self.year)
        search_fia = [("resums_inversio", "=", id_resum)]

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
            "vpi_retri", "estado"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                fia = O.GiscedataCnmcFiabilitat.read(item, fields_to_read)
                output = [
                    fia["codi"],
                    fia["finalitat"],
                    fia["id_instalacio"],
                    fia["cini"],
                    fia["codi_tipus_inst"],
                    fia["ccaa"],
                    fia["any_apm"],
                    fia["vol_total_inv"],
                    fia["ajudes"],
                    fia["inv_financiada"],
                    fia["vpi_retri"],
                    fia["estado"],
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
