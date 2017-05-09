#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.core import MultiprocessBased


class MACRO(MultiprocessBased):
    """
    Class to generate F2 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(MACRO, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """
        id_resum = get_resum_any_id(self.connection, self.year)
        search_pos = [("resums_inversio", "=", id_resum)]

        return self.connection.GiscedataCnmcMacro.search(search_pos)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "anio_periodo", "crec_pib", "pib_prev", "limite_sector",
            "inc_demanda_sector"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                macro = O.GiscedataCnmcMacro.read(item, fields_to_read)
                output = [
                    macro["anio_periodo"],
                    macro["crec_pib"],
                    macro["pib_prev"],
                    macro["limite_sector"],
                    macro["inc_demanda_sector"]
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
