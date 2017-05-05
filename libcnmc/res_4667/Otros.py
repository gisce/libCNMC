#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.core import MultiprocessBased


class Otros(MultiprocessBased):
    """
    Class to generate F3 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(Otros, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        search_params = []
        return self.connection.GiscedataCnmcAltres.search(search_params)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "codi", "finalitat", "identificador_py", "cini", "codigo_ccaa",
            "any_apm", "vol_total_inv", "ajudes", "inv_financiada",
            "vpi_retri", "estado"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                otro = O.GiscedataCnmcAltres.read(item, fields_to_read)
                output = [
                    otro["codi"],
                    otro["finalitat"],
                    otro["identificador_py"],
                    otro["cini"],
                    otro["codigo_ccaa"],
                    otro["any_apm"],
                    otro["vol_total_inv"],
                    otro["ajudes"],
                    otro["inv_financiada"],
                    otro["vpi_retri"],
                    otro["estado"],
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
