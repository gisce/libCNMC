#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import get_forced_elements


class FB3_2(StopMultiprocessBased):

    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        super(FB3_2, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB3_2 - POSICIONES EN SUBESTACIÓN'
        self.base_object = 'POSICIONS'
        self.all_int = kwargs.get('all_int', True)

    def get_sequence(self):
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
        
        forced_ids = get_forced_elements(self.connection, "giscedata.cts.subestacions.posicio")
        
        ids = self.connection.GiscedataCtsSubestacionsPosicio.search(
            search_params, 0, 0, False, {'active_test': False})

        ids = ids + forced_ids["include"]
        ids = list(set(ids) - set(forced_ids["exclude"]))

        return list(set(ids))

    def get_tensio(self, sub):
        o = self.connection
        res = ''
        if sub['tensio']:
            tensio_obj = o.GiscedataTensionsTensio.read(sub['tensio'][0])
            res = tensio_obj['tensio']
        return res

    def get_subestacio(self, sub_id):
        o = self.connection
        sub = o.GiscedataCtsSubestacions.read(sub_id, ['ct_id'])
        res = ''
        if sub:
            res = sub['ct_id'][1]
        return res

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'name', 'cini', 'propietari', 'subestacio_id', 'data_pm', 'tensio',
            'parc_id'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                sub = o.GiscedataCtsSubestacionsPosicio.read(
                    item, fields_to_read
                )
                o_subestacio = self.get_subestacio(sub['subestacio_id'][0])
                if sub['parc_id']:
                    o_parc = sub['parc_id'][1]
                else:
                    o_parc = sub['subestacio_id'][1] + "-"\
                        + str(self.get_tensio(sub))

                o_pos = sub['name']
                o_cini = sub['cini']
                o_prop = int(sub['propietari'])
                if not sub['data_pm']:
                    o_data = ""
                else:
                    o_data = datetime.strptime(sub['data_pm'], "%Y-%m-%d")
                    o_data = int(o_data.year)
                o_any = self.year

                self.output_q.put([
                    o_subestacio,   # SUBESTACION
                    o_parc,         # PARQUE
                    o_pos,          # POSICION
                    o_cini,         # CINI
                    o_prop,         # PROPIEDAD
                    o_data,         # FECHA PUESTA EN SERVICIO
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
