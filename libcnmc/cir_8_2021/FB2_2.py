#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback

from libcnmc.utils import format_f
from libcnmc.core import MultiprocessBased

TIPUS_INST = {
    'TI-174': 1,
    'TI-187': 1,
    'TI-182': 1,
    'TI-183': 1,
    'TI-187A': 1,
    'TI-179': 0,
    'TI-177': 0,
    'TI-181': 2,
    'TI-102V': 2,
}

class FB2_2(MultiprocessBased):
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

        search_params = [
            ("installacio", "like", "%giscedata.cts%"),
            ("tipus_element.codi", "!=", "FUS_AT")
        ]


        data_pm = '{}-01-01'.format(self.year + 1)
        data_baixa = '{}-12-31'.format(self.year)
        search_params += ['|', ('data_pm', '=', False),
                          ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                          ('data_baixa', '=', False)
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                          ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataCellesCella.search(
            search_params, 0, 0, False, {'active_test': False})

    def obtenir_ct(self, ct):
        i = 0
        res = 'a'
        es_ct = False
        if ct:
            while i < len(ct):
                if str(ct[i]) == "'" and not es_ct:
                    es_ct = True
                elif str(ct[i]) == "'" and es_ct:
                    es_ct = False
                if es_ct and str(ct[i]) != "'":
                    res += str(ct[i])
                i += 1
        return res

    def get_codi_ct(self, ct_id):
        o = self.connection
        ct = o.GiscedataCts.read(ct_id, ['name'])
        res = ''
        if ct:
            res = ct['name']
        return res

    def get_codi_maquina(self, ct_id):
        o = self.connection
        trafos = o.GiscedataCts.read(
            ct_id, ['transformadors'])['transformadors']
        trafo_id = o.GiscedataTransformadorTrafo.search(
            [('id', 'in', trafos), ('id_estat.codi', '=', 1)], 0, 1)
        res = ''
        if trafo_id:
            codi_maquina = o.GiscedataTransformadorTrafo.read(trafo_id[0],
                                                              ['name'])
            res = codi_maquina['name']
        return res

    def get_tipus_inst(self, ti_cnmc_id):
        o = self.connection
        ti_cnmc = o.GiscedataTipusInstallacio.read(
            ti_cnmc_id[0], ['name'])['name']
        return ti_cnmc

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'installacio', 'name', 'propietari', 'data_pm', 'cini', 'tipus_instalacio_cnmc_id'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                celles = o.GiscedataCellesCella.read(
                    item, fields_to_read
                )
                o_ct_id = int(celles['installacio'].split(',')[1])
                o_ct = self.get_codi_ct(o_ct_id)
                o_maquina = self.get_codi_maquina(o_ct_id)
                o_id_cella = celles['name']
                o_cini = celles['cini'] or ''
                o_propietari = int(celles['propietari'])
                o_data = ''
                if celles['data_pm']:
                    o_data = datetime.strptime(celles['data_pm'], "%Y-%m-%d")
                    o_data = int(o_data.year)

                o_interruptor = self.get_tipus_inst(celles['tipus_instalacio_cnmc_id'])
                o_interruptor_val = TIPUS_INST[o_interruptor]

                self.output_q.put([
                    o_ct,                # CT
                    o_id_cella,          # IDENTIFICADOR_CELDA
                    o_maquina,           # IDENTIFICADOR_MAQUINA
                    o_cini,              # CINI
                    o_interruptor_val,   # INTERRUPTOR
                    o_propietari,        # PROPIEDAD
                    o_data,              # FECHA PUESTA EN SERVICIO
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()