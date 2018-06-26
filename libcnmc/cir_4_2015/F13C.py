# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import re
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP
from libcnmc.utils import get_ine, get_comptador
from libcnmc.core import MultiprocessBased


class F13c(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F13c, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F13 C - CTS'
        self.base_object = 'CTS'
        self.mode = kwargs.get('all_int', True)

    def get_sequence(self):

        if self.mode:
            search_params = [
                ("interruptor", "in", (2, 3))
            ]
        else:
            search_params = [
                ("interruptor", "=", 2)
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
                    o_subestacio,
                    o_parc,
                    o_pos,
                    o_cini,
                    o_prop,
                    o_data,
                    o_any
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
