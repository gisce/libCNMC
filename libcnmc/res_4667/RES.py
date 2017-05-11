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

        model_resum = self.connection.GiscedataCnmcResum_any
        ids_resums = get_resum_any_id(self.connection, self.year)

        return ids_resums
        r_fields = ["anyo"]
        data = self.connection.GiscedataCnmcResum_any.read(ids_resums, r_fields)
        for line in data:
            self.years.append(line["anyo"])

        self.limite_empresa = dict.fromkeys(self.years, 0)
        self.demanda_empresa_p0 = dict.fromkeys(self.years, 0)
        self.inc_demanda_empresa_prv = dict.fromkeys(self.years, 0)
        self.frri = dict.fromkeys(self.years, 0)
        self.vpi_superado_prv = dict.fromkeys(self.years, 0)
        self.vol_total_inv_prv = dict.fromkeys(self.years, 0)
        self.ayudas_prv = dict.fromkeys(self.years, 0)
        self.financiacion_prv = dict.fromkeys(self.years, 0)
        self.vpi_retribuible_prv = dict.fromkeys(self.years, 0)
        self.num_proyectos = dict.fromkeys(self.years, 0)
        self.vol_total_inv_bt_prv = dict.fromkeys(self.years, 0)

        for line in model_resum.read(ids_resums, []):
            year = line["anyo"]

            self.limite_empresa[year] += line["limit_empresa"]
            self.demanda_empresa_p0[year] += line["demanda_empresa_p0"]
            self.inc_demanda_empresa_prv[year] += line["inc_demanda"]
            self.frri[year] += line["frri"]
            self.vpi_superado_prv[year] += line["vpi_sup"]
            self.vol_total_inv_prv[year] += line["volum_total_inv"]
            self.ayudas_prv[year] += line["ajudes_prev"]
            self.financiacion_prv[year] += line["financiacio"]
            self.vpi_retribuible_prv[year] += line["vpi_retribuible_prv"]
            self.num_proyectos[year] += line["n_projectes"]
            self.vol_total_inv_bt_prv[year] += ["voltotal_inv_bt_prv"]

        return [1]

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

                macro = O.GiscedataCnmcResum_any.read(item, fields_to_read)
                output = [
                    macro["anyo"],
                    format_f(macro["limit_empresa"], 3),
                    format_f(macro["demanda_empresa_p0"], 3),
                    format_f(macro["inc_demanda"], 3),
                    format_f(macro["frri"], 3),
                    macro["vpi_sup"],
                    format_f(macro["volum_total_inv"], 3),
                    format_f(macro["ajudes_prev"], 3),
                    format_f(macro["financiacio"], 3),
                    format_f(macro["vpi_retribuible_prv"], 3),
                    macro["n_projectes"],
                    format_f(macro["voltotal_inv_bt_prv"], 3)
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
