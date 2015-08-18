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

    def get_sequence(self):
        search_params = [('interruptor', '=', '2')]
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params += [('propietari', '=', True),
                          '|', ('data_pm', '=', False),
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

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'name', 'cini', 'propietari', 'tipus_posicio', 'data_pm', 'tensio',
            'subestacio_id'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                sub = o.GiscedataCtsSubestacionsPosicio.read(
                    item, fields_to_read
                )
                o_subestacio = sub['name']
                o_parc = sub['subestacio_id'][1] + "-"\
                    + str(self.get_tensio(sub))
                o_pos = sub['tipus_posicio']
                o_cini = sub['cini']
                o_prop = int(sub['propietari'])
                o_data = sub['data_pm']
                o_any = self.year + 1

                self.output_q.put([
                    o_subestacio,
                    o_parc,
                    o_pos,
                    o_cini,
                    o_prop,
                    o_data,
                    o_any
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
