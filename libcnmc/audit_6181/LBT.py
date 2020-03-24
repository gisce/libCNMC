#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback
from os import environ

from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_name_ti, format_f, get_codi_actuacio


class LBT(MultiprocessBased):
    """
    Class to generate installation report for LBT (2)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """
        self.year = kwargs.pop("year")
        self.prefix = kwargs.pop('prefix', 'B') or 'B'
        self.price_accuracy = int(environ.get('OPENERP_OBRES_PRICE_ACCURACY', '3'))
        super(LBT, self).__init__(**kwargs)
        if kwargs.get("include_header", False):
            self.file_header = self.get_header()

    def get_header(self):
        return [
            'IDENTIFICADOR',
            'CINI',
            'TIPO_INVERSION',
            'ORIGEN',
            'DESTINO',
            'CODIGO_CCUU',
            'CODIGO_CCAA_1',
            'CODIGO_CCAA_2',
            'NIVEL_TENSION_EXPLOTACION',
            'NUMERO_CIRCUITOS',
            'NUMERO_CONDUCTORES',
            'LONGITUD',
            'INTENSIDAD_MAXIMA',
            'SECCION',
            'FINANCIADO',
            'TIPO_SUELO',
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
            [], self.year, [2]
        )

        return installations_ids[2]

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
            'origen',
            'destino',
            'ccuu',
            'codigo_ccaa_1',
            'codigo_ccaa_2',
            'nivel_tension_explotacion',
            'numero_circuitos',
            'numero_conductores',
            'longitud',
            'intensidad_maxima',
            'seccion',
            'financiado',
            'tipo_suelo',
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

                linia = O.GiscedataProjecteObraTiBt.read([item], fields_to_read)[0]
                output = [
                    '{}{}'.format(self.prefix, linia['name']),
                    linia['cini'],
                    linia['tipo_inversion'],
                    linia['origen'],
                    linia['destino'],
                    get_name_ti(O, linia['ccuu'] and linia['ccuu'][0]),
                    linia['codigo_ccaa_1'],
                    linia['codigo_ccaa_2'],
                    linia['nivel_tension_explotacion'],
                    linia['numero_circuitos'],
                    linia['numero_conductores'],
                    linia['longitud'],
                    linia['intensidad_maxima'],
                    linia['seccion'],
                    linia['financiado'],
                    linia['tipo_suelo'],
                    linia['planificacion'],
                    linia['fecha_aps'],
                    linia['fecha_baja'],
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
