#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import get_codigo_ccaa, format_f
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

        self.year = kwargs.pop("year")
        super(Otros, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """
        id_resum = get_resum_any_id(self.connection, self.year)
        search_otro = [("resums_inversio", "in", id_resum)]

        return self.connection.GiscedataCnmcAltres.search(search_otro)

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
                    otro["codi"][1],
                    otro["finalitat"],
                    otro["identificador_py"],
                    otro["cini"],
                    get_codigo_ccaa(O, otro["codigo_ccaa"][0]),
                    otro["any_apm"],
                    format_f(otro["vol_total_inv"], 2) or "0.00",
                    format_f(otro["ajudes"], 2) or "0.00",
                    format_f(otro["inv_financiada"], 2) or "0.00",
                    format_f(otro["vpi_retri"], 2) or "0.00",
                    otro["estado"],
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
