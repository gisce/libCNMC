# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased


class F12bis(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F12bis, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F12 Bis - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        search_params = [
            # ('inventari', '=', 'l2+p'),
            # ('installacio', 'like', 'giscedata.cts')
        ]
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
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

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'installacio', 'name', 'propietari', 'data_pm', 'cini'
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
                o_posicio = celles['name']
                o_cini = celles['cini'] or ''
                o_propietari = int(celles['propietari'])
                o_data = ''
                if celles['data_pm']:
                    o_data = datetime.strptime(celles['data_pm'], "%Y-%m-%d")
                    o_data = int(o_data.year)
                o_any = self.year + 1

                self.output_q.put([
                    o_ct,
                    o_maquina,
                    o_posicio,
                    o_cini,
                    o_propietari,
                    o_data,
                    o_any
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
