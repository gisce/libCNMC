#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback
from os import environ

from libcnmc.utils import get_name_ti, format_f, get_codi_actuacio, \
    format_ccaa_code, convert_spanish_date
from libcnmc.core import MultiprocessBased


class LAT(MultiprocessBased):
    """
    Class to generate installation report for LAT (1)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """
        self.year = kwargs.pop('year')
        self.prefix = kwargs.pop('prefix', 'A') or 'A'
        self.price_accuracy = int(environ.get('OPENERP_OBRES_PRICE_ACCURACY', '2'))
        super(LAT, self).__init__(**kwargs)
        self.include_obres = False
        if kwargs.get("include_obra", False):
            self.include_obres = True
        if kwargs.get("include_header", False):
            self.file_header = self.get_header()

    def get_header(self):
        header = [
            'IDENTIFICADOR',
            'CINI',
            'TIPO_INVERSION',
            'ORIGEN',
            'DESTINO',
            'CODIGO_CCUU',
            'CODIGO_CCAA_1',
            'CODIGO_CCAA_2',
            'NUM_APOYO_TOTAL',
            'NUM_APOYO_SUSPENSE',
            'NUM_APOYO_AMARRE',
            'VELOCIDAD_VIENTO',
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
        if self.include_obres:
            header.append('IDENTIFICADOR_OBRA')
        return header

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        installations_ids = self.connection.GiscedataProjecteObra.get_audit_installations_by_year(
            [], self.year, [1]
        )

        return installations_ids[1]

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
            'num_apoyo_total',
            'num_apoyo_suspension',
            'num_apoyo_amarre',
            'velocidad_viento',
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
            'obra_id'
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataProjecteObraTiAt.read([item], fields_to_read)[0]
                output = [
                    '{}{}'.format(self.prefix, linia['name']),
                    linia['cini'],
                    linia['tipo_inversion'],
                    linia['origen'],
                    linia['destino'],
                    get_name_ti(O, linia['ccuu'] and linia['ccuu'][0]),
                    format_ccaa_code(linia['codigo_ccaa_1']),
                    format_ccaa_code(linia['codigo_ccaa_2']),
                    linia['num_apoyo_total'],
                    linia['num_apoyo_suspension'],
                    linia['num_apoyo_amarre'],
                    linia['velocidad_viento'],
                    format_f(linia['nivel_tension_explotacion']),
                    linia['numero_circuitos'],
                    linia['numero_conductores'],
                    format_f((linia['longitud'] or 0.0) / 1000.0),
                    linia['intensidad_maxima'],
                    linia['seccion'],
                    format_f(linia['financiado']),
                    linia['tipo_suelo'],
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
                    get_codi_actuacio(O, linia.get('motivacion') and linia['motivacion'][0]),
                ]
                if self.include_obres:
                    output.append(linia['obra_id'][1])
                output = map(lambda e: e or '', output)
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
