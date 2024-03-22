#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback

from libcnmc.utils import format_f
from libcnmc.core import StopMultiprocessBased


class FB2_2(StopMultiprocessBased):
    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(FB2_2, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB2_2 Bis - Cel·les'
        self.base_object = 'Cel·les'
        self.fiabilitat = kwargs.get("fiabilitat")
        self.doslmesp = kwargs.get("doslmesp")

    def get_sequence(self):
        """
        Generates a list of elements of celles to be passed to the consumer

        :return: None
        """
        cella_obj = self.connection.GiscedataCellesCella
        search_params = [('cini', '=like', 'I28%')]

        data_pm = '{}-01-01'.format(self.year + 1)
        data_baixa = '{}-12-31'.format(self.year)
        search_params += [('criteri_regulatori', '!=', 'excloure'),
                          '|', ('data_pm', '=', False),
                          ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                          ('data_baixa', '=', False)
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                          ('data_baixa', '!=', False),
                          ('active', '=', True)]

        cell_ids = cella_obj.search(search_params, 0, 0, False, {'active_test': False})
        return cell_ids

    def get_codi_ct(self, ct_id):
        o = self.connection
        ct = o.GiscedataCts.read(ct_id, ['name'])
        res = ''
        if ct:
            res = ct['name']
        return res

    def get_codi_maquina(self, ct_id, o_id_cella):
        o = self.connection
        res = ''
        cella_id = o.GiscedataCellesCella.search([('name', '=', o_id_cella)], 0, 0, False, {'active_test': False})
        tipus_pos_data = o.GiscedataCellesCella.read(cella_id[0], ['tipus_posicio'])
        if tipus_pos_data.get('tipus_posicio', False):
            tipus_pos_id = tipus_pos_data['tipus_posicio'][0]
            codi_pos = o.GiscedataCellesTipusPosicio.read(tipus_pos_id, ['codi'])
            if codi_pos['codi'] == 'P':
                trafos = o.GiscedataCts.read(
                    ct_id, ['transformadors'])['transformadors']
                trafo_id = o.GiscedataTransformadorTrafo.search(
                    [('id', 'in', trafos), ('id_estat.codi', '=', 1)], 0, 1)
                res = ''
                if trafo_id:
                    codi_maquina = o.GiscedataTransformadorTrafo.read(trafo_id[0],
                                                                      ['name', 'id_regulatori'])
                    res = codi_maquina['id_regulatori'] if codi_maquina.get('id_regulatori', False) \
                        else codi_maquina['name']
        return res

    def get_tipus_inst(self, ti_cnmc_id):
        o = self.connection
        ti_cnmc = o.GiscedataTipusInstallacio.read(
            ti_cnmc_id[0], ['name'])['name']
        return ti_cnmc

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'installacio', 'name', 'propietari', 'data_pm', 'cini', 'tipus_element'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                celles = o.GiscedataCellesCella.read(
                    item, fields_to_read
                )

                o_ct_id = int(celles['installacio'].split(',')[1])
                o_ct = self.get_codi_ct(o_ct_id)

                o_id_cella = celles['name']
                o_maquina = self.get_codi_maquina(o_ct_id, o_id_cella)
                o_cini = celles['cini'] or ''
                o_propietari = int(celles['propietari'])
                if o_propietari == 0:
                    o_maquina = ''
                o_interruptor_val = o.model('giscedata.celles.tipus.element').read(
                    celles['tipus_element'][0], ['interruptor']
                )['interruptor']

                o_data = ''
                if celles['data_pm']:
                    o_data = datetime.strptime(celles['data_pm'], "%Y-%m-%d")
                    o_data = int(o_data.year)


                self.output_q.put([
                    o_ct,                # CT
                    o_id_cella,          # IDENTIFICADOR_CELDA
                    o_maquina,           # IDENTIFICADOR_MAQUINA
                    o_cini,              # CINI
                    o_interruptor_val,   # INTERRUPTOR
                    o_propietari,        # PROPIEDAD
                    o_data,              # FECHA PUESTA EN SERVICIO
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
