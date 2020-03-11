#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback
from os import environ

from libcnmc.utils import format_f, get_codi_actuacio
from libcnmc.core import MultiprocessBased


class DES(MultiprocessBased):
    """
    Class to generate installation report for DES (6)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs:
        :type kwargs: dict
        """

        self.year = kwargs.pop("year")
        self.price_accuracy = int(environ.get('OPENERP_OBRES_PRICE_ACCURACY', '3'))
        super(DES, self).__init__(**kwargs)
        if kwargs.get("include_header", False):
            self.file_header = self.get_header()

    def get_header(self):
        return [
            'IDENTIFICADOR',
            'CINI',
            'FINANCIADO',
            'CODIGO_CCAA',
            'FECHA_APS',
            'FECHA_BAJA',
            'CAUSA_BAJA',
            'IM_INGENIERIA',
            'IM_MATERIALES',
            'IM_OBRACIVIL' 
            'IM_TRABAJOS',
            'SUBVENCIONES_EUROPEAS',
            'SUBVENCIONES_NACIONALES',
            'VALOR_AUDITADO',
            'VALOR_CONTABLE',
            'CUENTA_CONTABLE',
            'PORCENTAJE_MODIFICACION',
            'MOTIVACION',
        ]

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        installations_ids = self.connection.GiscedataProjecteObra.get_audit_installations_by_year(
            [], self.year, [6]
        )

        return installations_ids[6]

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            'name',
            'cini',
            'financiado',
            'codigo_ccaa',
            'fecha_aps',
            'fecha_baja',
            'causa_baja',
            'im_ingenieria',
            'im_materiales',
            'im_obracivil',
            'im_trabajos',
            'subvenciones_europeas',
            'subvenciones_nacionales',
            'valor_auditado',
            'valor_contabilidad',
            'cuenta_contable',
            'porcentaje_modificacion',
            'motivacion',
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataProjecteObraTiDespatx.read([item], fields_to_read)[0]
                output = [
                    linia['name'],
                    linia['cini'],
                    linia['financiado'],
                    linia['codigo_ccaa'],
                    linia['fecha_aps'],
                    linia['fecha_baja'],
                    linia['causa_baja'],
                    linia['im_ingenieria'],
                    linia['im_materiales'],
                    linia['im_obracivil'],
                    linia['im_trabajos'],
                    format_f(linia['im_ingenieria'] or 0.0, self.price_accuracy),
                    format_f(linia['im_materiales'] or 0.0, self.price_accuracy),
                    format_f(linia['im_obracivil'] or 0.0, self.price_accuracy),
                    format_f(linia['im_trabajos'] or 0.0, self.price_accuracy),
                    format_f(linia['subvenciones_europeas'] or 0.0, self.price_accuracy),
                    format_f(linia['subvenciones_nacionales'] or 0.0, self.price_accuracy),
                    format_f(linia['valor_auditado'] or 0.0, self.price_accuracy),
                    format_f(linia['valor_contabilidad'] or 0.0, self.price_accuracy),
                    linia['cuenta_contable'],
                    linia['porcentaje_modificacion'],
                    get_codi_actuacio(O, linia['motivacion'] and linia['motivacion'][0]),
                ]
                output = map(lambda e: e or '', output)
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
