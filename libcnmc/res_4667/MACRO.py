#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f


class MACRO(MultiprocessBased):
    """
    Class to generate F2 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        self.year = kwargs.pop('year')
        super(MACRO, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """
        ids_resum = get_resum_any_id(self.connection, self.year)

        return ids_resum

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "anyo", "macro_crec_pib", "macro_pib_prev", "macro_limite_sector",
            "macro_inc_demanda_sector"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                macro = O.GiscedataCnmcResum_any.read(item, fields_to_read)
                output = [
                    macro["anyo"],
                    format_f(macro["macro_crec_pib"], 4),
                    format_f(macro["macro_pib_prev"], 3),
                    format_f(macro["macro_limite_sector"], 3),
                    format_f(macro["macro_inc_demanda_sector"], 3)
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
