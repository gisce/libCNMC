#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback
from os import environ

from libcnmc.utils import format_f, get_name_ti, get_codi_actuacio, \
    format_ccaa_code, convert_spanish_date
from libcnmc.core import MultiprocessBased


class POS(MultiprocessBased):
    """
    Class to generate installation report for POS (4)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        self.year = kwargs.pop("year")
        self.price_accuracy = int(environ.get('OPENERP_OBRES_PRICE_ACCURACY', '2'))
        super(POS, self).__init__(**kwargs)
        if kwargs.get("include_header", False):
            self.file_header = self.get_header()

    def get_header(self):
        return [
            'IDENTIFICADOR',
            'CINI',
            'TIPO_INVERSION',
            'DENOMINACION',
            'CODIGO_CCUU',
            'CODIGO_CCAA',
            'IDENTIFICADOR_PARQUE',
            'NIVEL_TENSION_EXPLOTACION',
            'FINANCIADO',
            'PLANIFICACION',
            'FECHA_APS',
            'FECHA_BAJA',
            'CAUSA_BAJA',
            'IM_INGENIERIA',
            'IM_MATERIALES',
            'IM_OBRACIVIL',
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
            [], self.year, [4]
        )

        return installations_ids[4]

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
            'tipo_inversion',
            'denominacion',
            'ccuu',
            'codigo_ccaa',
            'identificador_parque',
            'nivel_tension_explotacion',
            'financiado',
            'planificacion',
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

                linia = O.GiscedataProjecteObraTiPosicio.read([item], fields_to_read)[0]
                output = [
                    linia['name'],
                    linia['cini'],
                    linia['tipo_inversion'],
                    linia['denominacion'],
                    get_name_ti(O, linia['ccuu'] and linia['ccuu'][0]),
                    format_ccaa_code(linia['codigo_ccaa']),
                    linia['identificador_parque'],
                    linia['nivel_tension_explotacion'],
                    format_f(linia['financiado']),
                    linia['planificacion'],
                    convert_spanish_date(linia['fecha_aps']),
                    convert_spanish_date(linia['fecha_baja']),
                    linia['causa_baja'],
                    format_f(linia['im_ingenieria'] or 0.0, self.price_accuracy),
                    format_f(linia['im_materiales'] or 0.0, self.price_accuracy),
                    format_f(linia['im_obracivil'] or 0.0, self.price_accuracy),
                    format_f(linia['im_trabajos'] or 0.0, self.price_accuracy),
                    format_f(linia['subvenciones_europeas'] or 0.0, self.price_accuracy),
                    format_f(linia['subvenciones_nacionales'] or 0.0, self.price_accuracy),
                    format_f(linia['valor_auditado'] or 0.0, self.price_accuracy),
                    format_f(linia['valor_contabilidad'] or 0.0, self.price_accuracy),
                    linia['cuenta_contable'],
                    format_f(linia['porcentaje_modificacion'] or 0.0),
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
