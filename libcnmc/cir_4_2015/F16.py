# -*- coding: utf-8 -*-
from datetime import datetime
import traceback

from libcnmc.utils import get_ine, format_f, convert_srid, get_srid
from libcnmc.core import MultiprocessBased


class F16(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F16, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F16 - Condensadors'
        self.base_object = 'Condensadors'

    def get_sequence(self):
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params = ['|', ('data_pm', '=', False),
                         ('data_pm', '<', data_pm),
                         '|', ('data_baixa', '>', data_baixa),
                         ('data_baixa', '=', False)]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataCondensadors.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_node_vertex(self, ct_id):
        O = self.connection
        bloc = O.GiscegisBlocsCtat.search([('ct', '=', ct_id)])
        node = ''
        vertex = None
        if bloc:
            bloc = O.GiscegisBlocsCtat.read(bloc[0], ['node', 'vertex'])
            if not bloc['node']:
                return '', ''
            node = bloc['node'][1]
            if bloc['vertex']:
                v = O.GiscegisVertex.read(bloc['vertex'][0], ['x', 'y'])
                vertex = (round(v['x'], 3), round(v['y'], 3))
        return node, vertex

    def get_ine(self, municipi_id):
        O = self.connection
        muni = O.ResMunicipi.read(municipi_id, ['ine', 'dc'])
        return get_ine(O, muni['ine'])

    def get_dades_ct(self, ct_id):
        O = self.connection
        ct = O.GiscedataCts.read(ct_id, ['id_municipi', 'propietari'])
        return ct

    def get_tensio(self, tensio_id):
        O = self.connection
        tensio = O.GiscedataTensionsTensio.read(tensio_id, ['tensio'])
        return tensio['tensio']

    def consumer(self):
        o_codi_r1 = 'R1-%s' % self.codi_r1[-3:]
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'ct_id', 'tensio_id', 'potencia_instalada'
        ]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                cond = O.GiscedataCondensadors.read(item, fields_to_read)
                o_node, vertex = self.get_node_vertex(cond['ct_id'][0])
                o_cond = cond['name']
                o_cini = cond['cini'] or ''
                ct = self.get_dades_ct(cond['ct_id'][0])
                o_ine_muni, o_ine_prov = '', ''
                if ct['id_municipi']:
                    o_ine_prov, o_ine_muni = self.get_ine(ct['id_municipi'][0])
                o_tensio = format_f(
                    float(self.get_tensio(cond['tensio_id'][0])) / 1000.0,
                    decimals=3
                )
                o_potencia = cond['potencia_instalada']
                o_propietari = int(ct['propietari'])
                o_any = self.year
                z = ''
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(
                        self.codi_r1, get_srid(O), vertex)
                self.output_q.put([
                    o_node,
                    o_cond,
                    o_cini,
                    format_f(res_srid[0], decimals=3),
                    format_f(res_srid[1], decimals=3),
                    z,
                    o_ine_muni,
                    o_ine_prov,
                    o_tensio,
                    o_potencia,
                    o_codi_r1,
                    o_propietari,
                    o_any
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
