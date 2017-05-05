#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
import traceback

from libcnmc.core import MultiprocessBased


class DES(MultiprocessBased):
    """
    Class to generate F6 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(DES, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        search_params = []
        return self.connection.GiscedataCnmcDespatx.search(search_params)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "codi", "finalitat", "id_instalacio", "cini", "codigo_ccaa",
            "any_apm", "vol_total_inv", "ajudes", "vpi_retri", "estado"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                des = O.GiscedataCnmcDespatx.read(item, fields_to_read)
                output = [
                    des["codi"],
                    des["finalitat"],
                    des["id_instalacio"],
                    des["cini"],
                    des["codigo_ccaa"],
                    des["any_apm"],
                    des["vol_total_inv"],
                    des["ajudes"],
                    des["vpi_retri"],
                    des["estado"]
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
