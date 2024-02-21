#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.res_4667.utils import get_resum_any_id
from libcnmc.utils import format_f, get_codigo_ccaa
from libcnmc.core import StopMultiprocessBased


class PRO(StopMultiprocessBased):
    """
    Class to generate proyectos of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs:
        :type kwargs: dict
        """

        self.year = kwargs.pop("year")

        self.proyectos = []

        self.vol_total_inv_prev_proy = {}
        self.ayudas_prv_proy = {}
        self.financiacion_prv_proy = {}
        self.vpi_retribuible_prv_proy = {}
        self.generated_proy = []
        self.ids = {}

        super(PRO, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        ids_resums = get_resum_any_id(self.connection, self.year)
        search_params = [("resums_inversio", "in", ids_resums)]

        model_proy = self.connection.GiscedataCnmcProjectes
        ids_proy = model_proy.search(search_params)
        for proy in model_proy.read(ids_proy, ["codi"]):
            self.proyectos.append(proy["codi"])
        self.proyectos = list(set(self.proyectos))

        self.vol_total_inv_prev_proy = dict.fromkeys(self.proyectos, 0.0)
        self.ayudas_prv_proy = dict.fromkeys(self.proyectos, 0.0)
        self.financiacion_prv_proy = dict.fromkeys(self.proyectos, 0.0)
        self.vpi_retribuible_prv_proy = dict.fromkeys(self.proyectos, 0.0)
        self.ids = dict.fromkeys(self.proyectos, 0.0)

        fields_read = [
            "vol_total_inv_prev_proy",
            "ayudas_prv_proy",
            "financiacion_prv_proy",
            "vpi_retribuible_prv_proy",
            "codi"
        ]
        for proy in model_proy.read(ids_proy, fields_read):
            self.vol_total_inv_prev_proy[proy["codi"]] += proy["vol_total_inv_prev_proy"]
            self.ayudas_prv_proy[proy["codi"]] += proy["ayudas_prv_proy"]
            self.financiacion_prv_proy[proy["codi"]] += proy["financiacion_prv_proy"]
            self.vpi_retribuible_prv_proy[proy["codi"]] += proy["vpi_retribuible_prv_proy"]
            self.ids[proy["codi"]] = proy["id"]

        return self.ids.values()

    def consumer(self):
        """
        Generates the line of the file

        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = []

        while True:
            try:
                item = self.input_q.get()
                if item == "STOP":
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                pro = O.GiscedataCnmcProjectes.read(item, fields_to_read)
                codigo = pro["codi"]
                ccaa = 0
                ccaa_2 = 0
                if pro["ccaa"]:
                    ccaa = pro["ccaa"][0]
                if pro["ccaa_2"]:
                    ccaa_2 = pro["ccaa_2"][0]

                output = [
                    pro["codi"],
                    pro["name"],
                    get_codigo_ccaa(O, ccaa),
                    get_codigo_ccaa(O, ccaa_2),
                    pro["memoria"].replace("\n", " ")[:300] or "",
                    format_f(self.vol_total_inv_prev_proy[codigo], 2) or "0.00",
                    format_f(self.ayudas_prv_proy[codigo], 2) or "0.00",
                    format_f(self.financiacion_prv_proy[codigo], 2) or "0.00",
                    format_f(self.vpi_retribuible_prv_proy[codigo], 2) or "0.00",
                    pro["estado"]
                ]

                self.output_q.put(output)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
