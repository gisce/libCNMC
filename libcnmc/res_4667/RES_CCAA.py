#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import format_f, get_codigo_ccaa
from libcnmc.core import MultiprocessBased


class RESCCAA(MultiprocessBased):
    """
    Class to generate F2 of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """
        self.year = kwargs.pop("year")
        self.ccaas = []
        self.years = []
        self.ayudas = {}
        self.vol_inv = {}
        self.financiacion = {}
        self.vpi = {}
        self.num_proy = {}
        self.inv_bt = {}

        super(RESCCAA, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        self.ayudas = dict.fromkeys(self.years, 0)
        self.vol_inv = dict.fromkeys(self.years, 0)
        self.financiacion = dict.fromkeys(self.years, 0)
        self.vpi = dict.fromkeys(self.years, 0)
        self.inv_bt = dict.fromkeys(self.years, 0)
        self.num_proy = dict.fromkeys(self.years, 0)

        model_ccaa = self.connection.GiscedataCnmcResum_ccaa

        ids_resums = get_resum_any_id(self.connection, self.year)
        search_res = [("resums_inversio", "in", ids_resums)]

        ids_resums_ccaa = model_ccaa.search(search_res)

        r_fields = ["anyo"]
        data = self.connection.GiscedataCnmcResum_any.read(ids_resums, r_fields)
        for line in data:
            self.years.append(line["anyo"])

        for line in model_ccaa.read(ids_resums, ["codigo_ccaa"]):
            self.ccaas.append(line["codigo_ccaa"][0])
        self.ccaas = list(set(self.ccaas))
        for year in self.years:
            self.ayudas[year] = dict.fromkeys(self.ccaas, 0)
            self.vol_inv[year] = dict.fromkeys(self.ccaas, 0)
            self.financiacion[year] = dict.fromkeys(self.ccaas, 0)
            self.vpi[year] = dict.fromkeys(self.ccaas, 0)
            self.inv_bt[year] = dict.fromkeys(self.ccaas, 0)
            self.num_proy[year] = dict.fromkeys(self.ccaas, 0)

        for line in model_ccaa.read(ids_resums_ccaa, []):
            year = line["anio_periodo"]
            ccaa = line["codigo_ccaa"][0]

            self.ayudas[year][ccaa] += line["ayudas_prv_ccaa"]
            self.vol_inv[year][ccaa] += line["vol_total_inv_prv_ccaa"]
            self.financiacion[year][ccaa] += line["financiacion_prv_ccaa"]
            self.vpi[year][ccaa] += line["vpi_retribuible_prv_ccaa"]
            self.inv_bt[year][ccaa] += line["vol_total_inv_bt_prv_ccaa"]
            self.num_proy[year][ccaa] += line["num_proyectos_ccaa"]

        return [1]

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """
        self.input_q.get()
        O = self.connection
        try:
            for year in self.years:
                for ccaa in self.ccaas:
                    output = [
                        get_codigo_ccaa(O, ccaa),
                        year,
                        format_f(self.vol_inv[year][ccaa], 3),
                        format_f(self.ayudas[year][ccaa], 3),
                        format_f(self.financiacion[year][ccaa], 3),
                        format_f(self.vpi[year][ccaa], 3),
                        self.num_proy[year][ccaa],
                        format_f(self.inv_bt[year][ccaa], 3),
                    ]
                    self.output_q.put(output)
        except Exception:
            traceback.print_exc()
            if self.raven:
                self.raven.captureException()
        finally:
            self.input_q.task_done()
