#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.core import MultiprocessBased


class MAQ(MultiprocessBased):
    """
    Class to generate F5 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(MAQ, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """
        id_resum = get_resum_any_id(self.connection, self.year)
        search_maq = [("resums_inversio", "=", id_resum)]

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
            "ccaa", "any_apm", "capacidad_prv", "vol_total_inv", "ajudes",
            "inv_financiada", "vpi_retri", "estado"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                maq = O.GiscedataCnmcMaquines.read(item, fields_to_read)
                output = [
                    maq["codi"],
                    maq["finalitat"],
                    maq["id_instalacio"],
                    maq["cini"],
                    maq["codi_tipus_inst"],
                    maq["ccaa"],
                    maq["any_apm"],
                    maq["capacidad_prv"],
                    maq["vol_total_inv"],
                    maq["ajudes"],
                    maq["inv_financiada"],
                    maq["vpi_retri"],
                    maq["estado"]
                ]
                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
