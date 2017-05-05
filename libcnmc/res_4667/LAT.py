#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
import traceback

from libcnmc.core import MultiprocessBased


class LAT(MultiprocessBased):
    """
    Class to generate F1 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(LAT, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report
        
        :return: List of ids
        :rtype: list
        """

        search_params = []
        return self.connection.GiscedataCnmcLinies.search(search_params)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "codi", "finalitat", "id_instalacio", "cini", "ccaa", "ccaa_2",
            "any_apm", "capacidad_prv", "vol_total_inv", "ajudes",
            "inv_financiada", "vpi_retri", "estado"]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataCnmcLinies.read(item, fields_to_read)
                output = [
                    linia["codi"],
                    linia["finalitat"],
                    linia["id_instalacio"],
                    linia["codi_tipus_inst"],
                    linia["ccaa"],
                    linia["ccaa_2"],
                    linia["any_apm"],
                    linia["capacidad_prv"],
                    linia["vol_total_inv"],
                    linia["ajudes"],
                    linia["inv_financiada"],
                    linia["vpi_retri"],
                    linia["estadp"]
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
