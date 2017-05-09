#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.core import MultiprocessBased


class RES(MultiprocessBased):
    """
    Class to generate F2 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(RES, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        id_resum = get_resum_any_id(self.connection, self.year)

        return [id_resum]

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            "anyo", "limit_empresa", "demanda_empresa_p0", "inc_demanda",
            "frri", "vpi_sup", "volum_total_inv", "ajudes_prev", "financiacio",
            "vpi_retribuible_prv", "n_projectes", "voltotal_inv_bt_prv"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                macro = O.GiscedataCnmcResumAny.read(item, fields_to_read)
                output = [
                    macro["anyo"],
                    macro["limit_empresa"],
                    macro["demanda_empresa_p0"],
                    macro["inc_demanda"],
                    macro["frri"],
                    macro["vpi_sup"],
                    macro["volum_total_inv"],
                    macro["ajudes_prev"],
                    macro["financiacio"],
                    macro["vpi_retribuible_prv"],
                    macro["n_projectes"],
                    macro["voltotal_inv_bt_prv"]
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
