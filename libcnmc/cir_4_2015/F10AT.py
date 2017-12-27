# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.utils import format_f, tallar_text
from libcnmc.core import MultiprocessBased


class F10AT(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F10AT, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F10AT - CTS'
        self.base_object = 'AT'
        self.layer = 'LBT\_%'
        id_res_like = self.connection.ResConfig.search(
            [('name', '=', 'giscegis_btlike_layer')])
        if id_res_like:
            self.layer = self.connection.ResConfig.read(
                id_res_like, ['value'])[0]['value']

        ids_red = self.connection.GiscegisBlocsTransformadorsReductors.search([])
        data_nodes = self.connection.GiscegisBlocsTransformadorsReductors.read(ids_red, ["node"])
        self.nodes_red = [nod["node"][1] for nod in data_nodes if nod["node"]]

    def get_sequence(self):
        search_params = [('name', '!=', '1')]
        lines_id = self.connection.GiscedataAtLinia.search(search_params)
        search_params = [('name', '=', '1')]
        fict_line_id = self.connection.GiscedataAtLinia.search(
            search_params, 0, 0, False, {'active_test': False})
        return lines_id + fict_line_id

    def get_provincia(self, id_prov):
        o = self.connection
        res = ''
        ine = o.ResCountryState.read(id_prov, ['code'])['code']
        if ine:
            res = ine
        return res

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'name', 'cini', 'circuits', 'longitud_cad', 'linia', 'origen',
            'final', 'coeficient', 'cable', 'tensio_max_disseny','tensio_max_disseny_id'
        ]
        data_pm_limit = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        static_search_params = [
            ('cini', '!=', '0000000'),
            '|',
            ('data_pm', '=', False),
            ('data_pm', '<', data_pm_limit),
            '|',
            ('data_baixa', '>', data_baixa),
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
                linia = o.GiscedataAtLinia.read(
                    item, ['trams', 'tensio', 'municipi', 'propietari',
                           'provincia']
                )

                o_prop = linia['propietari'] and '1' or '0'
                search_params = [('id', 'in', linia['trams'])]
                search_params += static_search_params
                ids = o.GiscedataAtTram.search(
                    search_params, 0, 0, False, {'active_test': False})
                for at in o.GiscedataAtTram.read(ids, fields_to_read):
                    # Coeficient per ajustar longituds de trams
                    coeficient = at['coeficient'] or 1.0
                    # Comprovar el tipus del cable
                    fields_to_read_cable = [
                        'tipus', 'resistencia', 'reactancia',
                        'intensitat_admisible'
                    ]
                    cable = o.GiscedataAtCables.read(at['cable'][0],
                                                     fields_to_read_cable)
                    o_tipus = o.GiscedataAtTipuscable.read(cable['tipus'][0],
                                                           ['codi'])['codi']
                    # Si el tram tram es embarrat amb una longitud > 100
                    # no l'afegim
                    if o_tipus == 'E' and at['longitud_cad'] > 100:
                        continue
                    if o_tipus == 'E':
                        o_tipus = 'S'
                    # Agafem la tensió
                    if at.get('tensio_max_disseny_id', False):
                        o_nivell_tensio = at['tensio_max_disseny_id'][1]
                    elif 'tensio_max_disseny' in at:
                        o_nivell_tensio = at['tensio_max_disseny']
                    else:
                        o_nivell_tensio = linia["tensio"]
                    o_nivell_tensio = format_f(
                        float(o_nivell_tensio) / 1000.0, 3)
                    o_tram = 'A%s' % at['name']
                    res = o.GiscegisEdge.search(
                        [('id_linktemplate', '=', at['name']),
                         ('layer', 'not ilike', self.layer),
                         ('layer', 'not ilike', 'EMBARRA%BT%')
                         ])
                    if not res or len(res) > 1:
                        edge = {'start_node': (0, '%s_0' % at['name']),
                                'end_node': (0, '%s_1' % at['name'])}
                    else:
                        edge = o.GiscegisEdge.read(res[0], ['start_node',
                                                            'end_node'])

                    o_node_inicial = tallar_text(edge['start_node'][1], 20)
                    o_node_inicial = o_node_inicial.replace('*', '')
                    if o_node_inicial in self.nodes_red:
                        o_node_inicial = "{}-{}".format(o_node_inicial, o_nivell_tensio)

                    o_node_final = tallar_text(edge['end_node'][1], 20)
                    o_node_final = o_node_final.replace('*', '')
                    if o_node_final in self.nodes_red:
                        o_node_final = "{}-{}".format(o_node_final, o_nivell_tensio)
                    o_cini = at['cini']
                    o_provincia = ''
                    if linia['provincia']:
                        o_provincia = self.get_provincia(linia['provincia'][0])
                    o_longitud = format_f(
                            round(float(at['longitud_cad']) *
                                  coeficient / 1000.0, 3) or 0.001, decimals=3)
                    o_num_circuits = at['circuits']
                    o_r = format_f(
                        cable['resistencia'] * (float(at['longitud_cad']) *
                                                coeficient / 1000.0) or 0.0,
                        decimals=6)
                    o_x = format_f(
                        cable['reactancia'] * (float(at['longitud_cad']) *
                                               coeficient / 1000.0) or 0.0,
                        decimals=6)
                    o_int_max = format_f(
                        cable['intensitat_admisible'] or 0.0, decimals=3)
                    o_op_habitual = 1  # Tots son actius
                    o_cod_dis = 'R1-%s' % self.codi_r1[-3:]
                    o_any = self.year

                    self.output_q.put([
                        o_tram,
                        o_node_inicial,
                        o_node_final,
                        o_cini,
                        o_provincia,
                        o_nivell_tensio,
                        o_longitud,
                        o_num_circuits,
                        o_tipus,
                        o_r,
                        o_x,
                        o_int_max,
                        o_op_habitual,
                        o_cod_dis,
                        o_prop,
                        o_any
                    ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
