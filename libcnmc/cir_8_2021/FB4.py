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
        if self.all_int:
            search_params = [
                ("interruptor", "in", ('2', '3'))
            ]
        else:
            search_params = [
                ("interruptor", "=", '2')
            ]

        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params += ['|', ('data_pm', '=', False),
                          ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                          ('data_baixa', '=', False),
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                          ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataCtsSubestacionsPosicio.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_cts_data(self, sub_id):
        o = self.connection
        sub = o.GiscedataCts.search('id', '=', sub_id)
        res = ''
        if sub:
            res = sub['id'][0]
            cts = o.GiscedataCts.read(res, ['node_id', 'propietari'])

            print("cts")
            print(cts)
        return cts

    def consumer(self):
        """
        Generates the line of the file
        :return: Line
        :rtype: str
        """

        O = self.connection

        fields_to_read = [
            'name', 'cini', 'node_id', 'propietari', 'subestacio_id', 'data_pm', 'tensio',
            'parc_id'
        ]

        fields_to_read_obra = [
            'name', 'cini', 'tipo_inversion', 'denominacion', 'ccuu', 'codigo_ccaa', 'identificador_parque',
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
                pos = O.GiscedataCtsSubestacionsPosicio.read(
                    item, fields_to_read
                )

                obra_id = O.GiscedataProjecteObraTiCts.search([('element_ti_id', '=', ct['id'])])
                if obra_id:
                    linia = O.GiscedataProjecteObraTiPosicio.read(obra_id, fields_to_read_obra)[0]
                else:
                    linia = ''

                print("denominacion")
                print(linia['denominacion'])


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

                cts = get_cts_data(pos['subestacio_id'])

                if pos['parc_id']:
                    o_parc = pos['parc_id'][1]
                else:
                    o_parc = pos['subestacio_id'][1] + "-"\
                        + str(self.get_tensio(pos))



                output = [
                    pos['name'],  #IDENTIFICADOR_POSICION
                    pos['cini'],  #CINI
                    #NUDO
                    #CODIGO_CCUU
                    o_parc, #PARC_ID a PARC_NAME,  #IDENTIFICADOR EMPLAZAMIENTO
                    #AJENA
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
