# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.utils import format_f, convert_srid, get_srid
from libcnmc.core import MultiprocessBased

class F15(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F15, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'F15 - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        search_params = [
            ('installacio', 'ilike', 'giscedata.at.suport,%')
        ]
        return self.connection.GiscedataCellesCella.search(search_params)

    def get_node_vertex(self, suport):
        o = self.connection
        node = ''
        vertex = None
        if suport:
            bloc = o.GiscegisBlocsSuportsAt.search(
                [('numsuport', '=', suport)]
            )
            if bloc:
                bloc = o.GiscegisBlocsSuportsAt.read(
                    bloc[0], ['node', 'vertex'])
                v = o.GiscegisVertex.read(bloc['vertex'][0], ['x', 'y'])
                if bloc.get('node', False):
                    node = bloc['node'][1]
                else:
                    print "ERROR: NO hem trobat node: bloc %s vertex %s\n" % (bloc, v['id'])
                    node = v['id']
                if bloc.get('vertex', False):
                    vertex = (round(v['x'], 3), round(v['y'], 3))
        return node, vertex

    def get_node_vertex_tram(self, element_name):
        o = self.connection
        node = ''
        vertex = None
        tram_name = ''
        if element_name:
            # Search on the diferent models
            models = [o.GiscegisBlocsInterruptorat,
                      o.GiscegisBlocsFusiblesat,
                      o.GiscegisBlocsSeccionadorat,
                      o.GiscegisBlocsSeccionadorunifilar]
            for model in models:
                bloc = model.search([('codi', '=', element_name)]
            )
            if bloc:
                bloc = o.GiscegisBlocsSuportsAt.read(
                    bloc[0], ['node', 'vertex'])
                v = o.GiscegisVertex.read(bloc['vertex'][0], ['x', 'y'])
                if bloc.get('node', False):
                    node = bloc['node'][1]
                else:
                    print "ERROR: NO hem trobat node: bloc %s vertex %s\n" % (bloc, v['id'])
                    node = v['id']
                if bloc.get('vertex', False):
                    vertex = (round(v['x'], 3), round(v['y'], 3))
                 # busquem el tram
                polver_ids = o.GiscegisPolylineVertex.search(
                    [('vertex', '=', v['id'])])
                if polver_ids:
                    poly_id = o.GiscegisPolyline.search(
                        [('vertex_ids', 'in', polver_ids[0])])[0]
                    edge_id = o.GiscegisEdge.search(
                        [('polyline', '=', poly_id)])[0]
                    linktemplate = o.GiscegisEdge.read(
                        edge_id, ['id_linktemplate'])['id_linktemplate']
                    tram_id = o.GiscedataAtTram.search(
                        [('name', '=', linktemplate)])
                    tram_name = o.GiscedataAtTram.read(
                        tram_id, ['name'])[0]['name']
        return node, vertex, tram_name

    def obtenir_tram(self, installacio):
        o = self.connection
        valor = installacio.split(',')
        id_tram = int(valor[1])
        tram = o.GiscedataAtSuport.read(id_tram, ['name'])
        valor = tram['name']
        return valor

    def obtenir_camps_linia(self, installacio):
        o = self.connection
        valor = installacio.split(',')
        id_tram = int(valor[1])
        tram = o.GiscedataAtSuport.read(id_tram, ['linia'])
        linia_id = tram['linia']
        fields_to_read = [
            'municipi', 'provincia', 'tensio'
        ]
        linia = o.GiscedataAtLinia.read(int(linia_id[0]), fields_to_read)
        municipi = ''
        provincia = ''
        id_municipi = linia['municipi'][0]
        id_provincia = linia['provincia'][0]
        tensio = format_f(float(linia['tensio']) / 1000.0)
        if id_municipi and id_provincia:
            provincia = o.ResCountryState.read(id_provincia, ['code'])['code']
            municipi_dict = o.ResMunicipi.read(id_municipi, ['ine', 'dc'])
            municipi = '{}{}'.format(municipi_dict['ine'][-3:],
                                     municipi_dict['dc'])
        res = {
            'municipi': municipi,
            'provincia': provincia,
            'tensio': tensio
        }
        return res

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'installacio', 'cini', 'propietari', 'name'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                celles = o.GiscedataCellesCella.read(
                    item, fields_to_read
                )
                o_tram = self.obtenir_tram(celles['installacio'])
                dict_linia = self.obtenir_camps_linia(celles['installacio'])
                o_fiabilitat = celles['name']
                o_node, vertex, o_tram = self.get_node_vertex_tram(o_fiabilitat)
                o_cini = celles['cini']
                z = ''
                o_municipi = dict_linia.get('municipi')
                o_provincia = dict_linia.get('provincia')
                o_tensio = format_f(dict_linia.get('tensio'), decimals=3)
                o_cod_dis = 'R1-%s' % self.codi_r1[-3:]
                o_prop = int(celles['propietari'])
                o_any = self.year
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(
                        self.codi_r1, get_srid(o), vertex)
                self.output_q.put([
                    o_node,
                    o_fiabilitat,
                    o_tram,
                    o_cini,
                    format_f(res_srid[0], decimals=3),
                    format_f(res_srid[1], decimals=3),
                    z,
                    o_municipi,
                    o_provincia,
                    o_tensio,
                    o_cod_dis,
                    o_prop,
                    o_any
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
