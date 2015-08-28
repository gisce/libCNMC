# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO


class F9(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F9, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F9 - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        search_params = []
        return self.connection.GiscedataCupsPs.search(search_params)

    def get_geom(self, id_tram, net):
        o = self.connection
        model_edge = o.GiscegisEdge
        model_vertex = o.GiscegisPolylineVertex
        model_polyline = o.GiscegisPolyline
        data = []
        if net.lower() == 'at':

            ids_edges = model_edge.search(
                [('id_linktemplate', '=', id_tram),
                 ('layer', 'not ilike', '%\_BT\_%')]
            )
        else:
            ids_edges = model_edge.search(
                [('id_linktemplate', '=', id_tram),
                 ('layer', 'not ilike', '%\_AT\_%')]
            )
        edges = model_edge.read(ids_edges)
        if not edges:
            return []
        vertexs = model_polyline.read(edges[0]['polyline'][0])
        for punt in model_vertex.read(vertexs['vertex_ids']):
            data.append({'x': punt['x'], 'y': punt['y']})
        return data

    def conv_text(self, data):
        t = ''
        for line in data:
            t += '{0};{1};{2}\n'.format(line['x'], line['y'], 0)
        return t[:-1]

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'name'
        ]
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
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                linia = o.GiscedataAtLinia.read(item, ['trams'])
                search_params = [('id', 'in', linia['trams'])]
                search_params += static_search_params
                ids = o.GiscedataAtTram.search(
                    search_params, 0, 0, False, {'active_test': False})
                for at in o.GiscedataAtTram.read(ids, fields_to_read):
                    data = self.get_geom(at['name'], 'at')
                    data = self.conv_text(data)
                    if data:
                        self.output_q.put(['A' + str(at['name'])])
                        self.output_q.put([data])
                        self.output_q.put(['END'])
                for bt in o.GiscedataBtElement.read(ids, fields_to_read):
                    data = self.get_geom(bt['name'], 'at')
                    data = self.conv_text(data)
                    if data:
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
