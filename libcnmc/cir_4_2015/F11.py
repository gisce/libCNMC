# -*- coding: utf-8 -*-
from datetime import datetime
import traceback

from libcnmc.utils import get_ine, format_f
from libcnmc.core import MultiprocessBased


class F11(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F11, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F11 - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        search_params = []
        return self.connection.GiscedataCts.search(search_params)

    def get_node_vertex(self, ct_id):
        O = self.connection
        bloc = O.GiscegisBlocsCtat.search([('ct', '=', ct_id)])
        node = ''
        vertex = ('', '')
        if bloc:
            bloc = O.GiscegisBlocsCtat.read(bloc[0], ['node', 'vertex'])
            node = bloc['node'][0]
            if bloc['vertex']:
                v = O.GiscegisVertex.read(bloc['vertex'][0], ['x', 'y'])
                vertex = (round(v['x'], 3), round(v['y'], 3))
        return node, vertex

    def get_ine(self, municipi_id):
        O = self.connection
        muni = O.ResMunicipi.read(municipi_id, ['ine', 'dc'])
        return get_ine(O, muni['ine'])

    def get_sortides_ct(self, ct_name):
        O = self.connection
        search = '%s-' % ct_name
        sortides = O.GiscegisBlocsFusiblesbt.search(
            [('codi', 'ilike', search)]
        )
        disponibles = len(sortides)
        utilitzades = 0
        for sortida in O.GiscegisBlocsFusiblesbt.read(sortides, ['node']):
            if sortida['node']:
                node = sortida['node'][0]
                edges = O.GiscegisEdge.search(
                    ['|', ('start_node', '=', node), ('end_node', '=', node)]
                )
                if len(edges) > 1:
                    utilitzades += 1
        return disponibles, utilitzades

    def get_tipus(self, subtipus_id):
        o = self.connection
        tipus = ''
        subtipus = o.GiscedataCtsSubtipus.read(subtipus_id, ['categoria_cne'])
        if subtipus['categoria_cne']:
            cne_id = subtipus['categoria_cne'][0]
            cne = o.GiscedataCneCtTipus.read(cne_id, ['codi'])
            tipus = cne['codi']
        return tipus

    def get_saturacio(self, ct_id):
        o = self.connection
        saturacio = ''
        if 'giscedata.transformadors.saturacio' in o.models:
            sat_obj = o.GiscedataTransformadorsSaturacio
            trafo_obj = o.GiscedataTransformadorTrafo
            sat = sat_obj.search([
                ('ct.id', '=', ct_id)
            ])
            saturacio = 0
            for sat in sat_obj.read(sat, ['b1_b2', 'trafo']):
                trafo = trafo_obj.read(sat['trafo'][0], ['potencia_nominal'])
                saturacio += trafo['potencia_nominal'] * sat['b1_b2']
            saturacio *= 0.9
        return saturacio

    def consumer(self):
        o_codi_r1 = 'R1-%s' % self.codi_r1[-3:]
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'id_municipi',   'tensio_p', 'id_subtipus',
            'perc_financament', 'propietari', 'numero_maxim_maquines',
            'potencia'
        ]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                ct = O.GiscedataCts.read(item, fields_to_read)
                o_node, vertex = self.get_node_vertex(item)
                o_ct = ct['name']
                o_cini = ct['cini'] or ''
                if ct['id_municipi']:
                    o_ine_prov, o_ine_muni = self.get_ine(ct['id_municipi'][0])
                else:
                    o_ine_muni, o_ine_prov = '', ''
                o_tensio_p = ct['tensio_p'] or ''
                if ct['id_subtipus']:
                    o_tipo = self.get_tipus(ct['id_subtipus'][0])
                else:
                    o_tipo = ''
                o_potencia = ct['potencia']
                cups = O.GiscedataCupsPs.search([('et', '=', ct['name'])])
                o_energia = sum(
                    x['cne_anual_activa']
                    for x in O.GiscedataCupsPs.read(
                        cups, ['cne_anual_activa']
                    )
                )
                o_pic_activa = self.get_saturacio(ct['id'])
                o_pic_reactiva = ''
                o_s_utilitades, o_s_disponibles = self.get_sortides_ct(
                    ct['name']
                )
                o_propietari = int(ct['propietari'])
                o_num_max_maquines = ct['numero_maxim_maquines']
                o_incorporacio = str(datetime.now().year)
                x = ''
                y = ''
                z = ''
                if vertex[0]:
                    x = format_f(float(vertex[0]), 3)
                if vertex[1]:
                    y = format_f(float(vertex[1]), 3)

                self.output_q.put([
                    o_node,
                    o_ct,
                    o_cini,
                    x,
                    y,
                    z,
                    o_ine_muni,
                    o_ine_prov,
                    o_tensio_p,
                    o_tipo,
                    o_potencia,
                    o_energia,
                    o_pic_activa,
                    o_pic_reactiva,
                    o_s_utilitades,
                    o_s_utilitades,
                    o_codi_r1,
                    o_propietari,
                    o_incorporacio,
                    o_num_max_maquines
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
