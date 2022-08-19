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
        self.all_int = kwargs.get('all_int', True)


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
        sub = o.GiscedataCtsSubestacions.search('id', '=', sub_id)
        cts_id = o.GiscedataCtsSubestacions.read(sub['id'][0], ['ct_id'])
        print("cts_id")
        print(cts_id)
        if cts_id:
            cts = o.GiscedataCts.search('id', '=', cts_id)
            cts_data = o.GiscedataCts.read(cts, ['node_id', 'propietari'])
            print("cts")
            print(cts_data)
        return cts_data

    def get_parc_name(self, parc_id):
        o = self.connection
        parc = o.GiscedataParcs.search('id', '=', parc_id)
        if parc:
            parc_name = o.GiscedataParcs.read(parc, ['name'])
        return parc_name

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

                obra_id = O.GiscedataProjecteObraTiPosicio.search([('element_ti_id', '=', pos['id'])])
                if obra_id:
                    linia = O.GiscedataProjecteObraTiPosicio.read(obra_id, fields_to_read_obra)[0]
                else:
                    linia = ''

                #print("denominacion")
                #print(linia['denominacion'])

                if linia != '':
                    im_materiales = format_f_6181(linia['im_materiales'] or 0.0, float_type='euro')
                    im_obracivil = format_f_6181(linia['im_obracivil'] or 0.0, float_type='euro')
                    im_construccion = str(
                        float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", "."))
                    ).replace(".", ",")

                    im_ingenieria = format_f_6181(linia['im_ingenieria'] or 0.0, float_type='euro')
                    im_trabajos = format_f_6181(linia['im_trabajos'] or 0.0, float_type='euro')

                    tipo_inversion = (linia['tipo_inversion'] or '0') if not linia['fecha_baja'] else '1'

                    valor_auditado = format_f_6181(linia['valor_auditado'] or 0.0, float_type='euro')
                    subvenciones_europeas = format_f_6181(linia['subvenciones_europeas'] or 0.0, float_type='euro')
                    subvenciones_nacionales = format_f_6181(linia['subvenciones_nacionales'] or 0.0, float_type='euro')

                    cuenta_contable = linia['cuenta_contable']
                    financiado = format_f_6181(linia['financiado'], float_type='decimal')
                    motivacion = get_codi_actuacio(O, linia['motivacion'] and linia['motivacion'][0])

                    identificador_baja = (
                        get_inst_name(linia['identificador_baja'][0])  # IDENTIFICADOR_BAJA
                        if linia['identificador_baja'] else ''
                    )

                #    fecha_aps = convert_spanish_date(
                #        linia['fecha_aps'] if not linia['fecha_baja']
                #                              and linia['tipo_inversion'] != '1' else ''
                #    )
                fecha_aps = pos['data_pm']

                # Si la data APS es igual a l'any de la generació del fitxer,
                # la data APS sortirà en blanc

                #fecha_ip = '' if fecha_aps and int(fecha_aps.split('/')[2]) != self.year \
                #    else fecha_aps



                #cts = self.get_cts_data(pos['subestacio_id'])

                if pos['parc_id']:
                    identificador_emplazamiento = pos['parc_id'][1]
                else:
                    o_parc = pos['subestacio_id'][1] + "-"\
                        + str(self.get_tensio(pos))

                    identificador_emplazamiento = "SUBESTACIO_NAME"



                output = [
                    pos['name'],  #IDENTIFICADOR_POSICION
                    pos['cini'],  #CINI
                    #NUDO
                    #CODIGO_CCUU
                    identificador_emplazamiento, #PARC_ID a PARC_NAME,  #IDENTIFICADOR EMPLAZAMIENTO
                    #AJENA
                    #EQUIPADA
                    #ESTADO
                    #MODELO
                    #PUNTO_FRONTERA
                    fecha_aps,      #FECHA_APS
                    #convert_spanish_date(linia['fecha_baja']),  #FECHA_BAJA
                    #linia['causa_baja'],        #CAUSA_BAJA

                    #fecha_ip,    #fecha IP
                    tipo_inversion,  # TIPO_INVERSION

                    im_ingenieria,    #IM_TRAMITES
                    im_construccion,    #IM_CONSTRUCCION
                    im_trabajos,      #IM_TRABAJOS

                    valor_auditado,   #VALOR_AUDITADO
                    #VALOR RESIDUAL

                    subvenciones_europeas,    #SUBVENCIONES EUROPEAS
                    subvenciones_nacionales,  #SUBVECIONES NACIONALES
                    #SUBVENCIONES_PRTR
                    cuenta_contable,   #CUENTA
                    financiado,   #FINANCIADO
                    motivacion,    #MOTIVACION
                    identificador_baja, #IDENTIFICADOR BAJA
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
