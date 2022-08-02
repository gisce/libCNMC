#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f_6181, get_name_ti, get_codi_actuacio, \
    format_ccaa_code, convert_spanish_date


class FB4(MultiprocessBased):

    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        """
            Class constructor
            :param kwargs:
            """

        self.year = kwargs.pop("year")

        super(FB4, self).__init__(**kwargs)
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
            [], self.year, [4]
        )

        return installations_ids[4]

    def get_cts_propietari(self, sub_name):
        o = self.connection
        sub = o.GiscedataCts.search('name', '=', sub_name)
        res = ''
        if sub:
            res = sub['id'][0]
            cts = o.GiscedataCts.read(res, ['propietari'])
            res = cts['propietari'][1]
            print("res")
            print(res)
        return res

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
            'obra_id',
            'identificador_baja',
        ]

        def get_inst_name(element_id):
            vals = self.connection.GiscedataCtsSubestacionsPosicio.read(
                element_id, ['name'])
            return vals['name']

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataProjecteObraTiPosicio.read([item], fields_to_read)[0]


                propietari = self.get_cts_propietari(linia['denominacion'])

                print("propietari")
                print(propietari)

                fecha_aps = convert_spanish_date(
                    linia['fecha_aps'] if not linia['fecha_baja']
                                          and linia['tipo_inversion'] != '1' else ''
                )
                # Si la data APS es igual a l'any de la generació del fitxer,
                # la data APS sortirà en blanc
                fecha_aps = '' if fecha_aps and int(fecha_aps.split('/')[2]) != self.year \
                    else fecha_aps

                im_materiales = format_f_6181(linia['im_materiales'] or 0.0, float_type='euro')
                im_obracivil = format_f_6181(linia['im_obracivil'] or 0.0, float_type='euro')
                im_construccion = str(
                    float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", "."))
                ).replace(".", ",")

                output = [
                    linia['name'],  #IDENTIFICADOR_POSICION
                    linia['cini'],  #CINI
                    get_name_ti(O, linia['ccuu'] and linia['ccuu'][0]), #CODIGO_CCUU
                    linia['denominacion'],  #IDENTIFICADOR EMPLAZAMIENTO
                    propietari,     #AJENA
                    #EQUIPADA
                    #ESTADO
                    #MODELO
                    #PUNTO_FRONTERA
                    fecha_aps,      #FECHA_APS
                    convert_spanish_date(linia['fecha_baja']),  #FECHA_BAJA
                    #linia['causa_baja'],        #CAUSA_BAJA

                    #fecha IP
                    (linia['tipo_inversion'] or '0') if not linia['fecha_baja'] else '1',  # TIPO_INVERSION

                    format_f_6181(linia['im_ingenieria'] or 0.0, float_type='euro'),    #IM_TRAMITES
                    im_construccion,    #IM_CONSTRUCCION
                    format_f_6181(linia['im_trabajos'] or 0.0, float_type='euro'),      #IM_TRABAJOS

                    format_f_6181(linia['valor_auditado'] or 0.0, float_type='euro'),   #VALOR_AUDITADO
                    #VALOR RESIDUAL

                    format_f_6181(linia['subvenciones_europeas'] or 0.0, float_type='euro'),    #SUBVENCIONES EUROPEAS
                    format_f_6181(linia['subvenciones_nacionales'] or 0.0, float_type='euro'),  #SUBVECIONES NACIONALES
                    #SUBVENCIONES_PRTR
                    linia['cuenta_contable'],   #CUENTA
                    format_f_6181(linia['financiado'], float_type='decimal'),   #FINANCIADO
                    get_codi_actuacio(O, linia['motivacion'] and linia['motivacion'][0]),    #MOTIVACION
                    (
                         get_inst_name(linia['identificador_baja'][0])  #IDENTIFICADOR_BAJA
                         if linia['identificador_baja'] else ''
                    ),
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
