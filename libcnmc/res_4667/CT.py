#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
import traceback

from libcnmc.core import MultiprocessBased


class CT(MultiprocessBased):
    """
    Class to generate F6 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(CT, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        search_params = []
        return self.connection.GiscedataCnmcCt.search(search_params)

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
                    ct["codi"],
                    ct["finalitat"],
                    ct["id_instalacio"],
                    ct["cini"],
                    ct["codi_tipus_inst"],
                    ct["ccaa"],
                    ct["any_apm"],
                    ct["vol_total_inv"],
                    ct["ajudes"],
                    ct["inv_financiada"],
                    ct["vpi_retri"],
                    ct["estado"]
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
