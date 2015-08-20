# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.utils import format_f
from libcnmc.core import MultiprocessBased


class F13bis(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F13bis, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F13 bis - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params = ['|', ('data_pm', '=', False),
                         ('data_pm', '<', data_pm),
                         '|', ('data_baixa', '>', data_baixa),
                         ('data_baixa', '=', False)
                         ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        search_params += [('tensio.tensio', '!=', False)]
        return self.connection.GiscedataCtsSubestacionsPosicio.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_node(self, ct_id):
        o = self.connection
        bloc = o.GiscegisBlocsCtat.search([('ct', '=', ct_id)])
        node = ''
        if bloc:
            bloc = o.GiscegisBlocsCtat.read(bloc[0], ['node'])
            node = bloc['node'][0]
        return node

    def get_tipus_parc(self, sub_id):
        return 0

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'cini', 'propietari', 'subestacio_id', 'tensio'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                sub = o.GiscedataCtsSubestacionsPosicio.read(
                    item, fields_to_read
                )
                o_subestacio = sub['subestacio_id'][1]
                o_parc = sub['subestacio_id'][1] + "-" + sub['tensio'][1]
                o_node = self.get_node(item)
                o_cini = sub['cini']
                o_tipus = self.get_tipus_parc(sub['subestacio_id'][0])
                o_tensio = format_f(float(sub['tensio'][1]) / 1000.0, 3)
                o_prop = int(sub['propietari'])
                o_any = self.year + 1

                self.output_q.put([
                    o_subestacio,
                    o_parc,
                    o_node,
                    o_cini,
                    o_tipus,
                    o_tensio,
                    o_prop,
                    o_any
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
