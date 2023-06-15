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
        search_params = [
            ('reductor', '=', True),
            ('id_estat.cnmc_inventari', '=', True)
        ]
        return self.connection.GiscedataTransformadorTrafo.search(
            search_params
        )

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

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'ct', 'name', 'cini', 'propietari', 'id_estat',
            'conexions', 'id_operacio', 'data_pm'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                trafo = o.GiscedataTransformadorTrafo.read(
                    item, fields_to_read
                )
                o_subestacio = trafo['ct'][1]
                o_maquina = trafo['name']
                o_cini = trafo['cini']

                # IDENTIFICADOR_PARQUE_ALTA / IDENTIFICADOR_PARQUE_BAJA
                if trafo.get('conexions', False):
                    o_costat_alta = self.get_parcs(trafo)['o_costat_alta']
                    o_costat_baixa = self.get_parcs(trafo)['o_costat_baixa']

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
                    if o_cini[1] == '2' and o_cini[2] == '7' and o_cini[5] == '1' and o_cini[7] == '1':
                        o_operacio = 0

                self.output_q.put([
                    o_subestacio,  # SUBESTACION
                    o_maquina,  # MAQUINA
                    o_cini,  # CINI
                    o_costat_alta,  # PARQUE ALTA
                    o_costat_baixa,  # PARQUE BAJA
                    o_any_ps,  # AÑO INFORMACION
                    o_operacio # OPERACION
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
