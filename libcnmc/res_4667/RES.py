#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import format_f
from libcnmc.core import MultiprocessBased


class RES(MultiprocessBased):
    """
    Class to generate F2 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs: 
        :type kwargs: dict 
        """

        self.year = kwargs.pop("year")

        self.years = []

        self.limite_empresa = {}
        self.demanda_empresa_p0 = {}
        self.inc_demanda_empresa_prv = {}
        self.frri = {}
        self.vpi_superado_prv = {}
        self.vol_total_inv_prv = {}
        self.ayudas_prv = {}
        self.financiacion_prv = {}
        self.vpi_retribuible_prv = {}
        self.num_proyectos = {}
        self.vol_total_inv_bt_prv = {}

        super(RES, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        ids_resums = get_resum_any_id(self.connection, self.year)
        return ids_resums

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

                resumen = O.GiscedataCnmcResum_any.read(item, fields_to_read)
                vpi_superado_prv = str(resumen["vpi_sup"]).upper()
                demanda_empresa_p0 = format_f(resumen["demanda_empresa_p0"], 3)
                if len(demanda_empresa_p0) > 8:
                    demanda_empresa_p0 = format_f(
                        resumen["demanda_empresa_p0"], 1
                    )
                output = [
                    resumen["anyo"],
                    format_f(resumen["limit_empresa"], 2) or "0.00",
                    demanda_empresa_p0 or "0.00",
                    format_f(resumen["inc_demanda"], 3) or "0.00",
                    format_f(resumen["frri"], 3) or "0.00",
                    vpi_superado_prv,
                    format_f(resumen["volum_total_inv"], 2) or "0.00",
                    format_f(resumen["ajudes_prev"], 2) or "0.00",
                    format_f(resumen["financiacio"], 2) or "0.00",
                    format_f(resumen["vpi_retribuible_prv"], 2) or "0.00",
                    resumen["n_projectes"],
                    format_f(resumen["voltotal_inv_bt_prv"], 2) or "0.00",
                    format_f(resumen["vol_total_inv_gr_prv"], 2) or "0.00",
                    format_f(resumen["vol_total_inv_prv_prtr"], 2) or "0.00",
                    format_f(resumen["ayudas_prv_prtr"], 2) or "0.00",
                    format_f(resumen["financiacion_prv_prtr"], 2) or "0.00",
                    format_f(resumen["vpi_retribuible_prv_prtr"], 2) or "0.00",
                    resumen["num_proyectos_prtr"],
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
