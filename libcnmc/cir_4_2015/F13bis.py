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
        search_params = [('ct_id.propietari', '=', True),
                         '|', ('ct_id.data_pm', '=', False),
                         ('ct_id.data_pm', '<', data_pm),
                         '|', ('ct_id.data_baixa', '>', data_baixa),
                         ('ct_id.data_baixa', '=', False),
                         ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('ct_id.active', '=', False),
                          ('ct_id.data_baixa', '!=', False),
                          ('ct_id.active', '=', True)]
        # Revisem que tingui un parc assignat
        search_params += [('parc_id', '!=', False)]
        return self.connection.GiscedataCtsSubestacions.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_subestacio(self, sub_id):
        o = self.connection
        sub = o.GiscedataCtsSubestacions.read(sub_id, ['ct_id', 'cini'])
        ct_id = sub['ct_id'][0]
        cini = sub['cini']
        bloc_ids = o.GiscegisBlocsCtat.search([('ct', '=', ct_id)])
        node = ''
        if bloc_ids:
            bloc = o.GiscegisBlocsCtat.read(bloc_ids[0], ['node'])
            node = bloc['node'][1]
        else:
            print "ct id: {}".format(ct_id)
        return {'node': node, 'cini': cini}

    def get_parc(self, parc_id, data):
        o = self.connection
        res = ''
        if data == 'codi':
            res = o.GiscedataParcs.read(parc_id, ['name'])['name']
        elif data == 'tipus':
            res = o.GiscedataParcs.read(parc_id, ['tipus'])['tipus'] - 1
        elif data == 'tensio':
            tensio_id = o.GiscedataParcs.read(
                parc_id, ['tensio_id'])['tensio_id'][0]
            res = o.GiscedataTensionsTensio.read(
                tensio_id, ['tensio'])['tensio']
        return res

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'id', 'propietari', 'name', 'parc_id'
        ]
        dict_cts = {}
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                sub = o.GiscedataCtsSubestacions.read(
                    item, fields_to_read
                )
                o_subestacio = sub['name']
                # o_parc = sub['subestacio_id'][1] + "-" + sub['tensio'][1]
                o_parc = self.get_parc(sub['parc_id'][0], 'codi')
                subestacio = self.get_subestacio(sub['id'])
                o_node = subestacio['node']
                o_node = o_node.replace('*', '')
                o_cini = subestacio['cini']
                o_tipus = self.get_parc(sub['parc_id'][0], 'tipus')
                tensio = self.get_parc(sub['parc_id'][0], 'tensio')
                o_tensio = format_f(
                    float(tensio) / 1000.0, decimals=3)
                o_prop = int(sub['propietari'])
                o_any = self.year
                insert = True
                if o_subestacio not in dict_cts.keys():
                    dict_cts[o_subestacio] = [o_tensio]
                else:
                    llista_valors = dict_cts[o_subestacio]
                    for valor in llista_valors:
                        if valor == o_tensio:
                            insert = False
                    if insert:
                        dict_cts[o_subestacio].append(o_tensio)

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
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
