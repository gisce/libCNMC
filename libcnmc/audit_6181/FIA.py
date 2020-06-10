#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f_6181, get_name_ti, get_codi_actuacio, \
    convert_spanish_date, format_ccaa_code


class FIA(MultiprocessBased):
    """
    Class to generate installation report for FIA (7)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs: 
        :type kwargs: dict 
        """

        self.year = kwargs.pop("year")
        self.prefix = kwargs.pop('prefix', 'A') or 'A'

        super(FIA, self).__init__(**kwargs)
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
            'CODIGO_CCUU',
            'CODIGO_CCAA',
            'NIVEL_TENSION_EXPLOTACION',
            'ELEMENTO_ACT',
            'FINANCIADO',
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
            'IDENTIFICADOR_BAJA',
        ]
        if self.include_obres:
            header.insert(0, 'IDENTIFICADOR_OBRA')
        return header

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        installations_ids = self.connection.GiscedataProjecteObra.get_audit_installations_by_year(
            [], self.year, [7]
        )

        return installations_ids[7]

    def render_elemento_act(self, elemento_act):
        try:
            tram_at = self.connection.GiscedataAtTram.search(
                [('name', '=', elemento_act)])
            if tram_at:
                return '{}{}'.format(self.prefix, elemento_act)
        except:
            pass
        return elemento_act

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
            'ccuu',
            'codigo_ccaa',
            'nivel_tension_explotacion',
            'elemento_act',
            'financiado',
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
            'obra_id',
            'identificador_baja',
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataProjecteObraTiCelles.read([item], fields_to_read)[0]

                fecha_aps = convert_spanish_date(
                    linia['fecha_aps'] if not linia['fecha_baja'] else ''
                )
                # Si la data APS es igual a l'any de la generació del fitxer,
                # la data APS sortirà en blanc
                fecha_aps = '' if fecha_aps and int(fecha_aps.split('/')[2]) == self.year \
                    else fecha_aps
                output = [
                    linia['name'],
                    linia['cini'],
                    linia['tipo_inversion'] if not linia['fecha_baja'] else '',
                    get_name_ti(O, linia['ccuu'] and linia['ccuu'][0]),
                    format_ccaa_code(linia['codigo_ccaa']),
                    format_f_6181(linia['nivel_tension_explotacion'], float_type='decimal'),
                    self.render_elemento_act(linia['elemento_act']),
                    format_f_6181(linia['financiado'], float_type='decimal'),
                    fecha_aps,
                    convert_spanish_date(linia['fecha_baja']),
                    linia['causa_baja'],
                    format_f_6181(linia['im_ingenieria'] or 0.0, float_type='euro'),
                    format_f_6181(linia['im_materiales'] or 0.0, float_type='euro'),
                    format_f_6181(linia['im_obracivil'] or 0.0, float_type='euro'),
                    format_f_6181(linia['im_trabajos'] or 0.0, float_type='euro'),
                    format_f_6181(linia['subvenciones_europeas'] or 0.0, float_type='euro'),
                    format_f_6181(linia['subvenciones_nacionales'] or 0.0, float_type='euro'),
                    format_f_6181(linia['valor_auditado'] or 0.0, float_type='euro'),
                    format_f_6181(linia['valor_contabilidad'] or 0.0, float_type='euro'),
                    linia['cuenta_contable'],
                    (
                        format_f_6181(
                            max(linia['porcentaje_modificacion'] or 0.0, 100.0),
                            float_type='decimal'
                        )
                        if linia['tipo_inversion'] != '0' else ''
                    ) if not linia['fecha_baja'] else '',
                    get_codi_actuacio(O, linia['motivacion'] and linia['motivacion'][0]) 
                    if not linia['fecha_baja'] else '',
                    linia['identificador_baja'],
                ]
                if self.include_obres:
                    output.insert(0, linia['obra_id'][1])

                output = map(lambda e: '' if e is False or e is None else e, output)
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
