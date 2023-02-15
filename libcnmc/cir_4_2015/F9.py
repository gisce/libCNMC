# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import get_srid, convert_srid, format_f, parse_geom

try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO


class F9(StopMultiprocessBased):
    def __init__(self, **kwargs):
        super(F9, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F9 - Topologia real de la xarxa '
        self.base_object = 'Trams AT i BT'
        o = self.connection
        wiz_obj = o.WizardCircular4_2015
        self.codi_r1 = wiz_obj.default_get(['codi_r1'])['codi_r1']
        id_res_like = o.ResConfig.search(
            [('name', '=', 'giscegis_btlike_layer')])
        self.layer = 'LBT\_%'
        if id_res_like:
            self.layer = o.ResConfig.read(id_res_like, ['value'])[0]['value']
        self.alternative = kwargs.get('alternative', False)

    def get_sequence(self):
        """
        Generates the sequence of ids to be passed to the consummer function

        :return: ids to process
        :rtype: list[int]
        """

        o = self.connection
        data_pm_limit = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        static_search_params = ['|',
                                ('data_pm', '=', False),
                                ('data_pm', '<', data_pm_limit),
                                '|',
                                ('data_baixa', '>', data_baixa),
                                ('data_baixa', '=', False),
                                ]
        # Revisem que si està de baixa ha de tenir la data informada.
        static_search_params += ['|',
                                 '&',
                                 ('active', '=', False),
                                 ('data_baixa', '!=', False),
                                 ('active', '=', True)
                                 ]
        # AT
        trams = []
        ids_linia_at = o.GiscedataAtLinia.search([])
        fict_line_id = o.GiscedataAtLinia.search(
            [('name', '=', '1')], 0, 0, False, {'active_test': False})
        ids_linia_at += fict_line_id
        linies = o.GiscedataAtLinia.read(ids_linia_at, ['trams'])
        for elem in linies:
            trams += elem['trams']
        search_params = [('id', 'in', trams),
                         ('cini', '!=', '0000000')]
        search_params += static_search_params
        ids_at = o.GiscedataAtTram.search(
            search_params, 0, 0, False, {'active_test': False})
        search_params = [('id', 'in', trams),
                         ('cini', '!=', '0000000'),
                         ('longitud_cad', '>', 100),
                         ('cable.tipus.codi', '=', 'E')]
        search_params += static_search_params
        remove_ids_at = o.GiscedataAtTram.search(
            search_params, 0, 0, False, {'active_test': False})
        ids_at = list(set(ids_at)-set(remove_ids_at))
        # BT

        search_params = [
            ('cable.tipus.codi', 'in', ['T', 'D', 'S', 'E', 'I'])
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

        return ids

    def get_geom(self, id_tram, net):
        """
        Returns the geometry of the first point of the tram

        :param id_tram: Identifier of the tram
        :type id_tram: int
        :param net: type of network (AT,BT)
        :type net: str
        :return: X, Y
        :rtype: tuple[float,float]
        """

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
                 ('layer', 'not ilike', 'EMBARRA%BT%')]
            )
        else:
            ids_edges = model_edge.search(
                [('id_linktemplate', '=', id_tram),
                 '|',
                 ('layer', 'ilike', like_layer),
                 ('layer', 'ilike', 'EMBARRA%BT%')]
            )
        edges = model_edge.read(ids_edges)
        if not edges:
            return []
        vertexs = model_polyline.read(edges[0]['polyline'][0])
        for punt in model_vertex.read(vertexs['vertex_ids']):
            data.append({'x': punt['x'],
                         'y': punt['y']})
        return data

    def conv_text(self, data):
        """
        Converts the projection of a data

        :param data: list of elemnets to convert
        :return: Elements with the projection converted
        """
        o = self.connection
        t = ''
        for line in data:
            res_srid = convert_srid(get_srid(o), [line['x'], line['y']])
            t += '{0};{1};{2}\n'.format(
                format_f(res_srid[0], decimals=6),
                format_f(res_srid[1], decimals=6),
                '')
        return t[:-1]

    def conv_text_alt(self, data):
        """
        Converts the projection of a data

        :param data: list of elemnets to convert
        :return: Elements with the projection converted
        """
        o = self.connection
        t = ''
        for line in data:
            res_srid = convert_srid(get_srid(o), [line['x'], line['y']])
            t += 'R1-{0};t_name;{1};{2};{3};1\n'.format(
                self.codi_r1,
                format_f(res_srid[0], decimals=6),
                format_f(res_srid[1], decimals=6),
                ''
            )
        return t[:-1]

    def consumer(self):
        """
        Function that generates each line of the file

        :return: None
        :rtype: None
        """
        o = self.connection
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                if item == "STOP":
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                if item[1] == 'at':
                    if 'geom' in o.GiscedataAtTram.fields_get().keys():
                        at = o.GiscedataAtTram.read(item[0], ['geom', 'name'])
                        if at['geom']:
                            data = parse_geom(at['geom'])
                        else:
                            data = self.get_geom(at['name'], 'at')
                    else:
                        at = o.GiscedataAtTram.read(item[0], ['name'])
                        data = self.get_geom(at['name'], 'at')
                    if self.alternative:
                        data = self.conv_text_alt(data)
                        linia = data.replace('t_name', 'A' + str(at['name']))
                    else:
                        data = self.conv_text(data)
                        linia = 'A{0}\n{1}\nEND'.format(at['name'], data)
                else:
                    if 'geom' in o.GiscedataBtElement.fields_get().keys():
                        bt = o.GiscedataBtElement.read(
                            item[0], ['geom', 'name']
                        )
                        if bt['geom']:
                            data = parse_geom(bt['geom'])
                        else:
                            data = self.get_geom(bt['name'], 'bt')
                    else:
                        bt = o.GiscedataBtElement.read(item[0], ['name'])
                        data = self.get_geom(bt['name'], 'bt')
                    if self.alternative:
                        data = self.conv_text_alt(data)
                        linia = data.replace('t_name', 'B' + str(bt['name']))
                    else:
                        data = self.conv_text(data)
                        linia = 'B{0}\n{1}\nEND'.format(bt['name'], data)
                self.output_q.put([linia])
                self.input_q.task_done()
            except:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()

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
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.output_q.task_done()
        fio.write('END')
        if not self.file_output:
            self.content = fio.getvalue()
        fio.close()

        if not self.file_output:
            self.content = fio.getvalue()
        fio.close()
        if self.file_modificaciones:
            fio_mod = open(self.file_modificaciones, 'wb')
        else:
            fio_mod = StringIO()

        while True:
            try:
                val = self.output_m.get()
                if val == 'STOP':
                    break
                fio_mod.writelines(val + "\n")
                self.output_m.task_done()
            except:
                self.output_m.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
        fio_mod.close()
