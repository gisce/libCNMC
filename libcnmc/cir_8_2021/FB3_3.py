#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f, get_norm_tension

OPERACIO = {
    'Reserva': '0',
    'Operativo': '1',
}

class FB3_3(MultiprocessBased):

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

    def get_costat_alta(self, trafo):
        o = self.connection
        res = ''
        if trafo['conexions']:
            con = o.GiscedataTransformadorConnexio.read(trafo['conexions'][0])
            tensio = con['tensio_primari']
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

    def get_operacio(self, id_operacio):

        o = self.connection
        operacio = o.GiscedataTransformadorOperacio.read(
            id_operacio, ['descripcio']
        )[1]
        return operacio

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'ct', 'name', 'cini', 'propietari', 'id_estat',
            'conexions', 'id_operacio'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                trafo = o.GiscedataTransformadorTrafo.read(
                    item, fields_to_read
                )
                o_subestacio = trafo['ct'][1]
                o_maquina = trafo['name']
                o_cini = trafo['cini']
                o_costat_alta = self.get_costat_alta(trafo)
                o_costat_baixa = self.get_costat_baixa(trafo)
                o_propietat = int(trafo['propietari'])
                o_estat = self.get_estat(trafo['id_estat'][0])
                o_any = self.year

                id_operacio = trafo['id_operacio']
                if id_operacio:
                    desc_operacio = self.get_operacio(id_operacio)
                    if desc_operacio:
                        o_operacio = OPERACIO[desc_operacio]
                else:
                    o_operacio = ''

                # TODO: Treure aquesta linia
                desc_operacio = 'Operativo'
                o_operacio = OPERACIO[desc_operacio]

                self.output_q.put([
                    o_subestacio,  # SUBESTACION
                    o_maquina,  # MAQUINA
                    o_cini,  # CINI
                    o_costat_alta,  # PARQUE ALTA
                    o_costat_baixa,  # PARQUE BAJA
                    o_any,  # AÑO INFORMACION
                    o_operacio # OPERACION
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
