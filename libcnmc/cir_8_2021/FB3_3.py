#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f, get_norm_tension

OPERACIO = {
    'Reserva': '0',
    'Operativo': '1',
}

class FB3_3(StopMultiprocessBased):

    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        super(FB3_3, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB3_3 - TRAFOS-SE'
        self.base_object = 'TRAFOS'

    def get_sequence(self):

        data_pm = '{}-01-01'.format(self.year + 1)
        data_baixa = '{}-12-31'.format(self.year)

        criteris_dates = [('criteri_regulatori', '!=', 'excloure'),
                          '|', ('data_pm', '=', False),
                          ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                          ('data_baixa', '=', False)
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        criteris_dates += ['|',
                          '&', ('active', '=', False),
                          ('data_baixa', '!=', False),
                          ('active', '=', True)]

        search_params = [
            ('reductor', '=', True),
            ('id_estat.cnmc_inventari', '=', True)
        ] + criteris_dates

        trafo_ids = [
            'T.{}'.format(x)
            for x in self.connection.GiscedataTransformadorTrafo.search(
                search_params
            )
        ]
        # Condensadors
        search_params = [
            ('ct_id.id_installacio.name', '=', 'SE'),
            ('tipus', '=', '2'),
        ] + criteris_dates

        condensadors_ids = [
            'C.{}'.format(x) for x in
            self.connection.GiscedataCondensadors.search(search_params)
        ]
        return trafo_ids + condensadors_ids

    def get_estat(self, estat_id):
        o = self.connection
        estat = o.GiscedataTransformadorEstat.read(estat_id, ['codi'])
        if estat['codi'] != 1:
            return 0
        else:
            return 1

    def get_parcs(self, trafo):
        o = self.connection
        res = {'o_costat_alta': '',
               'o_costat_baixa': ''}
        conexions_ids = trafo['conexions']

        con_obj = o.GiscedataTransformadorConnexio
        se_obj = o.GiscedataCtsSubestacions
        parc_obj = o.GiscedataParcs
        tensio_obj = o.GiscedataTensionsTensio

        con_alta, con_baixa = False, False
        for connexio_id in conexions_ids:
            con_data = con_obj.read(connexio_id)
            if con_data['conectada']:
                con_alta = con_data['tensio_primari']
                if con_data['tensio_b1']:
                    con_baixa = con_data['tensio_b1']
                elif con_data['tensio_b2']:
                    con_baixa = con_data['tensio_b2']
                elif con_data['tensio_b3']:
                    con_baixa = con_data['tensio_b3']
                break

        if con_alta and con_baixa:
            tensions_ids = tensio_obj.search([('tipus', '=', 'AT')])
            tensio_alta, tensio_baixa = False, False
            for tensio_id in tensions_ids:
                llindar_inf = tensio_obj.read(tensio_id, ['l_inferior'])['l_inferior']
                llindar_sup = tensio_obj.read(tensio_id, ['l_superior'])['l_superior']
                if llindar_inf <= con_alta <= llindar_sup:
                    tensio_alta = tensio_id
                elif llindar_inf <= con_baixa <= llindar_sup:
                    tensio_baixa = tensio_id

            if tensio_alta and tensio_baixa:
                se_name = trafo['ct'][1]
                se_id = se_obj.search([('name', '=', se_name)])
                parc_id_alta = parc_obj.search([('subestacio_id', '=', se_id), ('tensio_id', '=', tensio_alta)])
                if parc_id_alta:
                    res['o_costat_alta'] = parc_obj.read(parc_id_alta[0], ['name'])['name']
                parc_id_baixa = parc_obj.search([('subestacio_id', '=', se_id), ('tensio_id', '=', tensio_baixa)])
                if parc_id_baixa:
                    res['o_costat_baixa'] = parc_obj.read(parc_id_baixa[0], ['name'])['name']

        return res

    def get_costat_baixa(self, trafo):
        o = self.connection
        res = ''
        if trafo['conexions']:
            con = o.GiscedataTransformadorConnexio.read(trafo['conexions'][0])
            tensio = con['tensio_b1']
            tensio_n = get_norm_tension(o, tensio)
            se_id = trafo['ct'][1]
            parc_id = o.GiscedataParcs.search(
                [
                    ('subestacio_id', '=', se_id),
                    ('tensio_id.tensio', '=', tensio_n)
                ]
            )
            if parc_id:
                res = o.GiscedataParcs.read(parc_id[0], ['name'])['name']
        return res

    def get_operacio(self, id_estat):

        o = self.connection
        operacio = o.GiscedataTransformadorEstat.read(
            id_estat[0], ['operacio']
        )['operacio']
        return operacio

    def process_trafo(self, trafo_id):
        fields_to_read = [
            'ct', 'name', 'cini', 'propietari', 'id_estat',
            'conexions', 'id_operacio', 'data_pm', 'id_regulatori'
        ]
        trafo = self.connection.GiscedataTransformadorTrafo.read(
            trafo_id, fields_to_read
        )
        o_subestacio = trafo['ct'][1]
        o_maquina = trafo['id_regulatori'] if trafo.get('id_regulatori',
                                                        False) else trafo[
            'name']
        o_cini = trafo['cini']

        # IDENTIFICADOR_PARQUE_ALTA / IDENTIFICADOR_PARQUE_BAJA
        if trafo.get('conexions', False):
            o_costat_alta = self.get_parcs(trafo)['o_costat_alta']
            o_costat_baixa = self.get_parcs(trafo)['o_costat_baixa']
        else:
            o_costat_alta = o_costat_baixa = ''

        o_propietat = int(trafo['propietari'])
        o_estat = self.get_estat(trafo['id_estat'][0])

        # AÑO_PS
        o_any_ps = ''
        if trafo.get('data_pm', False):
            o_any_data = str(trafo['data_pm'])
            o_any_ps = o_any_data.split('-')[0]

        id_estat = trafo['id_estat']
        if id_estat:
            operacio = self.get_operacio(id_estat)
            if operacio:
                o_operacio = operacio
            else:
                o_operacio = 0
        else:
            o_operacio = 0

        if o_cini:
            if o_cini[1] == '2' and o_cini[2] == '7' and o_cini[5] == '1' and \
                    o_cini[7] == '1':
                o_operacio = 0

        return [
            o_subestacio,  # SUBESTACION
            o_maquina,  # MAQUINA
            o_cini,  # CINI
            o_costat_alta,  # PARQUE ALTA
            o_costat_baixa,  # PARQUE BAJA
            o_any_ps,  # AÑO INFORMACION
            o_operacio  # OPERACION
        ]

    def process_condensador(self, condensador_id):
        fields_to_read = [
            'ct_id', 'name', 'cini', 'parc_alta', 'parc_baixa', 'data_pm'
        ]
        condensador = self.connection.GiscedataCondensadors.read(
            condensador_id, fields_to_read
        )
        return [
            condensador['ct_id'] and condensador['ct_id'][1] or '',
            condensador['name'],
            condensador['cini'],
            condensador['parc_alta'] and condensador['parc_alta'][1] or '',
            condensador['parc_baixa'] and condensador['parc_baixa'][1] or '',
            condensador['data_pm'] and condensador['data_pm'].split('-')[0] or '',
            1
        ]

    def consumer(self):

        while True:
            try:
                # generar linies
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                element_type, element_id = item.split('.')
                if element_type == 'T':
                    result = self.process_trafo(int(element_id))
                elif element_type == 'C':
                    result = self.process_condensador(int(element_id))
                else:
                    result = []
                if result:
                    self.output_q.put(result)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
