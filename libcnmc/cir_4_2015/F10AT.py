# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.utils import format_f, tallar_text
from libcnmc.core import StopMultiprocessBased


class F10AT(StopMultiprocessBased):
    def __init__(self, **kwargs):
        super(F10AT, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F10AT - LAT'
        self.base_object = 'AT'
        self.layer = 'LBT\_%'
        id_res_like = self.connection.ResConfig.search(
            [('name', '=', 'giscegis_btlike_layer')])
        if id_res_like:
            self.layer = self.connection.ResConfig.read(
                id_res_like, ['value'])[0]['value']
        o = self.connection
        trafos_fields = o.GiscedataTransformadorTrafo.fields_get().keys()
        if 'node_id' in trafos_fields:
            ids_red = o.GiscedataTransformadorTrafo.search(
                [('reductor', '=', True)]
            )
            data_nodes = o.GiscedataTransformadorTrafo.read(
                ids_red, ["node_id"]
            )
            self.nodes_red = [
                nod["node_id"][1] for nod in data_nodes if nod["node_id"]
            ]
        else:
            ids_red = o.GiscegisBlocsTransformadorsReductors.search([])
            data_nodes = o.GiscegisBlocsTransformadorsReductors.read(
                ids_red, ["node"]
            )
            self.nodes_red = [
                nod["node"][1] for nod in data_nodes if nod["node"]
            ]

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
            'final', 'coeficient', 'cable', 'tensio_max_disseny_id', 'id_regulatori'
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
                if item == "STOP":
                    self.input_q.task_done()
                    break
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
                        o_tipus = 'D'
                    # Agafem la tensió
                    if at.get('tensio_max_disseny_id', False):
                        nivell_tensio_id = at['tensio_max_disseny_id'][0]
                        o_nivell_tensio = o.GiscedataTensionsTensio.read(nivell_tensio_id, ["tensio"])["tensio"]
                    else:
                        o_nivell_tensio = linia["tensio"]
                    o_nivell_tensio = format_f(
                        float(o_nivell_tensio) / 1000.0, 3)

                    # Si hi ha 'id_regulatori' el posem
                    if at.get('id_regulatori', False):
                        o_tram = at['id_regulatori']
                    else:
                        o_tram = 'A%s' % at['name']

                    if 'edge_id' in o.GiscedataAtTram.fields_get().keys():
                        at_edge = o.GiscedataAtTram.read(
                            at['id'], ['edge_id']
                        )['edge_id']
                        if not at_edge:
                            res = o.GiscegisEdge.search(
                                [
                                    ('id_linktemplate', '=', at['name']),
                                    ('layer', 'not ilike', self.layer),
                                    ('layer', 'not ilike', 'EMBARRA%BT%')
                                ]
                            )
                            if not res or len(res) > 1:
                                edge = {
                                    'start_node': (0, '%s_0' % at['name']),
                                    'end_node': (0, '%s_1' % at['name'])
                                }
                            else:
                                edge = o.GiscegisEdge.read(
                                    res[0], ['start_node', 'end_node']
                                )
                        else:
                            edge = o.GiscegisEdge.read(
                                at_edge[0], ['start_node', 'end_node']
                            )
                    else:
                        res = o.GiscegisEdge.search(
                            [
                                ('id_linktemplate', '=', at['name']),
                                ('layer', 'not ilike', self.layer),
                                ('layer', 'not ilike', 'EMBARRA%BT%')
                            ]
                        )
                        if not res or len(res) > 1:
                            edge = {
                                'start_node': (0, '%s_0' % at['name']),
                                'end_node': (0, '%s_1' % at['name'])
                            }
                        else:
                            edge = o.GiscegisEdge.read(
                                res[0], ['start_node', 'end_node']
                            )
                    o_node_inicial = tallar_text(edge['start_node'][1], 20)
                    o_node_inicial = o_node_inicial.replace('*', '')
                    if o_node_inicial in self.nodes_red:
                        o_node_inicial = "{}-{}".format(
                            o_node_inicial, o_nivell_tensio
                        )
                    o_node_final = tallar_text(edge['end_node'][1], 20)
                    o_node_final = o_node_final.replace('*', '')
                    if o_node_final in self.nodes_red:
                        o_node_final = "{}-{}".format(
                            o_node_final, o_nivell_tensio
                        )
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
                        o_tram,             # TRAMO
                        o_node_inicial,     # NUDO INICIAL
                        o_node_final,       # NUDO FINAL
                        o_cini,             # CINI
                        o_provincia,        # PROVINCIA
                        o_nivell_tensio,    # NIVEL TENSION
                        o_longitud,         # LONGITUD
                        o_num_circuits,     # NUMERO CIRCUITOS
                        o_tipus,            # TIPO
                        o_r,                # RESISTENCIA
                        o_x,                # REACTANCIA
                        o_int_max,          # INTENSIDAD MAXIMA
                        o_op_habitual,      # ESTADO OPERACION HABITUAL
                        o_cod_dis,          # CODIGO DISTRIBUIDORA
                        o_prop,             # PROPIEDAD
                        o_any               # AÑO INFORMACION
                    ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
