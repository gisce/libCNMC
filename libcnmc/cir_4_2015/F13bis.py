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
        # Revisem que estigui actiu
        search_params = [('active', '!=', False)]
        return self.connection.GiscedataParcs.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_subestacio(self, sub_id):
        o = self.connection
        sub = o.GiscedataCtsSubestacions.read(sub_id, ['ct_id', 'cini', 'name'])
        ct_id = sub['ct_id'][0]
        cini = sub['cini']
        name = sub['name']
        bloc_ids = o.GiscegisBlocsCtat.search([('ct', '=', ct_id)])
        node = ''
        if bloc_ids:
            bloc = o.GiscegisBlocsCtat.read(bloc_ids[0], ['node'])
            node = bloc['node'][1]
        else:
            print("ct id: {}".format(ct_id))
        return {'node': node, 'cini': cini, 'name': name}

    def get_tensio(self, parc_id):
        o = self.connection
        tensio_id = o.GiscedataParcs.read(
            parc_id, ['tensio_id'])['tensio_id'][0]
        return o.GiscedataTensionsTensio.read(tensio_id, ['tensio'])['tensio']

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'id', 'subestacio_id', 'name', 'tipus', 'propietari', 'cini'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                parc = o.GiscedataParcs.read(
                    item, fields_to_read
                )
                subestacio = self.get_subestacio(parc['subestacio_id'][0])
                o_subestacio = subestacio['name']
                o_parc = parc['name']
                o_node = subestacio['node']
                o_node = o_node.replace('*', '')
                o_cini = parc['cini']
                o_tipus = parc['tipus'] - 1
                tensio = self.get_tensio(parc['id'])
                o_tensio = format_f(
                    float(tensio) / 1000.0, decimals=3)
                o_prop = int(parc['propietari'])
                o_any = self.year
                insert = True
                if insert:
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
            except Exception as e:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
