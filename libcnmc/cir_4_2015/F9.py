# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_srid, convert_srid, format_f
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO


class F9(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F9, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F9 - CTS'
        self.codi_r1 = ''
        self.base_object = 'CTS'
        o = self.connection
        wiz_obj = o.WizardCircular4_2015
        self.codi_r1 = wiz_obj.default_get(['codi_r1'])['codi_r1']
        id_res_like = o.ResConfig.search(
            [('name', '=', 'giscegis_btlike_layer')])
        self.layer = 'LBT\_%'
        if id_res_like:
            self.layer = o.ResConfig.read(id_res_like, ['value'])[0]['value']

    def get_sequence(self):
        o = self.connection
        data_pm_limit = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        static_search_params = ['|', ('data_pm', '=', False),
                                ('data_pm', '<', data_pm_limit),
                                '|', ('data_baixa', '>', data_baixa),
                                ('data_baixa', '=', False),
                                ]
        # Revisem que si està de baixa ha de tenir la data informada.
        static_search_params += ['|',
                                 '&', ('active', '=', False),
                                 ('data_baixa', '!=', False),
                                 ('active', '=', True)]
        # AT
        ids_at = 0
        trams = []
        ids_bt = 0
        ids_linia_at = o.GiscedataAtLinia.search([])
        linia = o.GiscedataAtLinia.read(ids_linia_at, ['trams'])
        for elem in linia:
            trams += elem['trams']
        search_params = [('id', 'in', trams)]
        search_params += static_search_params
        ids_at = o.GiscedataAtTram.search(
            search_params, 0, 0, False, {'active_test': False})

        # BT

        search_params = [
            ('cable.tipus.codi', 'in', ['T', 'D', 'S'])
        ]
        search_params += static_search_params
        ids_bt = o.GiscedataBtElement.search(
            search_params, 0, 0, False, {'active_test': False})

        # IDS AT + BT

        ids = []

        for at in ids_at:
            ids.append((at, 'at'))
        for bt in ids_bt:
            ids.append((bt, 'bt'))

        # print "ids AT: {0}".format(len(ids_at))
        # print "ids BT: {0}".format(len(ids_bt))

        return ids

    def get_geom(self, id_tram, net):
        o = self.connection
        like_layer = self.layer
        model_edge = o.GiscegisEdge
        model_vertex = o.GiscegisPolylineVertex
        model_polyline = o.GiscegisPolyline
        data = []
        if net.lower() == 'at':
            ids_edges = model_edge.search(
                [('id_linktemplate', '=', id_tram),
                 ('layer', 'not ilike', like_layer),
                 ('layer', 'not ilike', '%EMBARRAT%'),
                 ('layer', 'not ilike', '%EMBARRADO%')]
            )
            # print "ids edges at: {0}".format(len(ids_edges))
        else:
            ids_edges = model_edge.search(
                [('id_linktemplate', '=', id_tram),
                 ('layer', 'ilike', like_layer),
                 ('layer', 'not ilike', '%EMBARRAT%'),
                 ('layer', 'not ilike', '%EMBARRADO%')]
            )
            # print "ids edges bt: {0}".format(len(ids_edges))
        edges = model_edge.read(ids_edges)
        if not edges:
            return []
        vertexs = model_polyline.read(edges[0]['polyline'][0])
        for punt in model_vertex.read(vertexs['vertex_ids']):
            data.append({'x': punt['x'],
                         'y': punt['y']})
        return data

    def conv_text(self, data):
        o = self.connection
        t = ''
        for line in data:
            res_srid = convert_srid(self.codi_r1, get_srid(o),
                                    [line['x'], line['y']])
            t += '{0};{1};{2}\n'.format(
                format_f(res_srid[0], decimals=6),
                format_f(res_srid[1], decimals=6),
                0)
        return t[:-1]

    def consumer(self):
        o = self.connection
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                if item[1] == 'at':
                    at = o.GiscedataAtTram.read(item[0], ['name'])
                    data = self.get_geom(at['name'], 'at')
                    data = self.conv_text(data)
                    self.output_q.put(['A' + str(at['name'])])
                    self.output_q.put([data])
                    self.output_q.put(['END'])
                else:
                    bt = o.GiscedataBtElement.read(item[0], ['name'])
                    data = self.get_geom(bt['name'], 'bt')
                    data = self.conv_text(data)
                    self.output_q.put(['B' + str(bt['name'])])
                    self.output_q.put([data])
                    self.output_q.put(['END'])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                # print contents
                self.input_q.task_done()

    def writer(self):
        if self.file_output:
            fio = open(self.file_output, 'wb')
        else:
            fio = StringIO()
        while True:
            try:
                item = self.output_q.get()
                if item == 'STOP':
                    break
                msg = map(
                    lambda x: type(x) == unicode and x.encode('utf-8') or x,
                    item
                )
                fio.write(str(msg[0])+'\n')
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.output_q.task_done()
        fio.write('END')
        if not self.file_output:
            self.content = fio.getvalue()
        fio.close()
