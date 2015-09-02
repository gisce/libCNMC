# -*- coding: utf-8 -*-
from datetime import datetime
import traceback

from libcnmc.utils import format_f
from libcnmc.core import MultiprocessBased


class F12(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F12, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F12 - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params = [
            ('id_estat.cnmc_inventari', '=', True)
        ]
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
        return self.connection.GiscedataTransformadorTrafo.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_node(self, ct_id):
        o = self.connection
        bloc = o.GiscegisBlocsCtat.search([('ct', '=', ct_id)])
        node = ''
        if bloc:
            bloc = o.GiscegisBlocsCtat.read(bloc[0], ['node'])
            node = bloc['node'][0]
        return node

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'ct', 'name', 'cini', 'potencia_nominal', 'propietari',
            'perdues_buit', 'perdues_curtcircuit_nominal'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                trafo = o.GiscedataTransformadorTrafo.read(
                    item, fields_to_read
                )
                o_ct = trafo['ct'] or ''
                o_node = ''
                if o_ct:
                    o_node = self.get_node(o_ct[0])
                    o_ct = str(o_ct[1])
                o_cini = trafo['cini'] or ''
                o_maquina = trafo['name']
                o_pot = format_f(
                    trafo['potencia_nominal'] * 1000.0, decimals=3)
                o_perdues_buit = format_f(
                    trafo['perdues_buit'], decimals=3) or 0
                o_perdues_nominal = format_f(
                    trafo['perdues_curtcircuit_nominal'], decimals=3) or 0
                o_propietari = int(trafo['propietari'])
                o_any = self.year

                self.output_q.put([
                    o_node,
                    o_ct,
                    o_cini,
                    o_maquina,
                    o_pot,
                    o_perdues_buit,
                    o_perdues_nominal,
                    o_propietari,
                    o_any
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
