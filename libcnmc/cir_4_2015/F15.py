# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.utils import format_f, convert_srid, get_srid, fetch_cts_node, get_total_elements
from libcnmc.utils import fetch_tensions_norm, fetch_mun_ine, fetch_prov_ine
from libcnmc.core import MultiprocessBased
from shapely import wkt


class F15Pos(MultiprocessBased):
    """
    Class to generate the F15 (Posiciones)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param year: Generation year
        :param codi_r1: R1 code of the company
        """

        super(F15Pos, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.cod_dis = 'R1-{}'.format(self.codi_r1[-3:])
        self.tensions = fetch_tensions_norm(self.connection)
        self.cts = {}
        self.srid = str(get_srid(self.connection))
        self.provincias = fetch_prov_ine(self.connection)
        self.municipios = fetch_mun_ine(self.connection)
        self.cts_node = fetch_cts_node(self.connection)

    def get_sequence(self):
        """
        Generates the sequence of ids to pass to the consume function

        :return: List of ids to generate the
        :rtype: list(int)
        """
        pos_model = self.connection.GiscedataCtsSubestacionsPosicio
        search_params = [("interruptor", "=", "3")]
        ids = pos_model.search(search_params)

        return get_total_elements(self.connection, "giscedata.cts.subestacions.posicio", ids)

    def consumer(self):
        """
        Consumer function that generates each line of the file

        :return: None
        """

        while True:
            try:
                item = self.input_q.get()
                fields_read = [
                    "name", "tensio", "cini", "propietari", "x", "y",
                    "subestacio_id", "node_id"
                ]
                fields_sub_read = [
                    "x", "y", "ct_id","id_municipi","id_provincia"
                ]
                pos = self.connection.GiscedataCtsSubestacionsPosicio.read(
                    item, fields_read
                )

                sub = self.connection.GiscedataCtsSubestacions.read(
                    pos["subestacio_id"][0], fields_sub_read
                )

                point = [sub["x"], sub["y"]]
                point_25830 = convert_srid(self.codi_r1, self.srid, point)

                if "node_id" in sub:
                    nudo = sub["node_id"]
                else:
                    nudo = self.cts_node[sub["ct_id"][0]]
                self.output_q.put(
                    [
                        nudo,                           # Nudo
                        pos.get("name", ""),            # Elemento de fiabilidad
                        "",                             # Tramo
                        pos.get("cini", ""),            # CINI
                        format_f(point_25830[0], decimals=3),
                        format_f(point_25830[1], decimals=3),
                        0,
                        self.municipios[sub["id_municipi"][0]],  # Codigo INE de municipio
                        self.provincias[sub["id_provincia"][0]],  # Codigo de provincia INE
                        self.tensions.get(pos["tensio"][0], 0),  # Nivel de tension
                        self.cod_dis,  # Codigo de la compañia distribuidora
                        int(pos.get("propietari", 1)),  # Propiedad
                        self.year  # Año de inforacion
                     ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class F15Cel(MultiprocessBased):
    """
    Class to generate the F15 (Celdas)
    """

    def __init__(self, **kwargs):
        """
        Class constructor

        :param year: Year to generate
        :type year: int
        :param codi_r1:
        :type codi_r1:str
        """

        super(F15Cel, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'F15 - Celles'
        self.base_object = 'celles'
        self.cod_dis = 'R1-{}'.format(self.codi_r1[-3:])

    def get_sequence(self):
        """
        Generates the sequence of ids to pass to the consume function

        :return: List of ids to generate the
        :rtype: list(int)
        """
        search_params = [
            ("installacio", "like", "giscedata.at.suport"),
            ("tipus_element.codi_cnmc", "!=", "T")
        ]
        return self.connection.GiscedataCellesCella.search(search_params)

    def get_node_vertex_tram(self, element_name):
        o = self.connection
        node = ''
        vertex = None
        tram_name = ''
        bloc = None
        if element_name:
            # Search on the diferent models
            models = [o.GiscegisBlocsInterruptorat,
                      o.GiscegisBlocsFusiblesat,
                      o.GiscegisBlocsSeccionadorat,
                      o.GiscegisBlocsSeccionadorunifilar]
            for model in models:
                bloc_id = model.search([('codi', '=', element_name)])
                if bloc_id:
                    model_ok = model
                    break
            if bloc_id:
                bloc = model_ok.read(
                    bloc_id[0], ['node', 'vertex'])
                v = o.GiscegisVertex.read(bloc['vertex'][0], ['x', 'y'])
                if bloc.get('node', False):
                    node = bloc['node'][1]
                else:
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
                    tram_name = "A{0}".format(tram_name)
        return node, vertex, tram_name

    def get_node_vertex(self, element_name):
        o = self.connection
        node = ''
        vertex = None
        bloc = None
        if element_name:
            # Search on the diferent models
            models = [o.GiscegisBlocsInterruptorat,
                      o.GiscegisBlocsFusiblesat,
                      o.GiscegisBlocsSeccionadorat,
                      o.GiscegisBlocsSeccionadorunifilar]
            for model in models:
                bloc_id = model.search([('codi', '=', element_name)])
                if bloc_id:
                    model_ok = model
                    break
            if bloc_id:
                bloc = model_ok.read(
                    bloc_id[0], ['node', 'vertex'])
                v = o.GiscegisVertex.read(bloc['vertex'][0], ['x', 'y'])
                if bloc.get('node', False):
                    node = bloc['node'][1]
                else:
                    node = v['id']
                if bloc.get('vertex', False):
                    vertex = (round(v['x'], 3), round(v['y'], 3))

        return node, vertex

    def get_node_ct(self, element_id):
        o = self.connection
        node = ''
        search_params = [
            ('ct', '=', element_id)
        ]

        if element_id:
            bloc_id = o.GiscegisBlocsCtat.search(search_params)
            node_id = o.GiscegisBlocsCtat.read(bloc_id, ['node'])
            if node_id:
                node_id = node_id[0]
                if node_id:
                    node_id = node_id['node']
                    if node_id:
                        node_id = node_id[0]
                        node = o.GiscegisNodes.read(node_id, ['name'])
                        if node:
                            node = node['name']
                    else:
                        node = ''
                else:
                    node = ''
            else:
                node = ''

        return str(node)

    def obtenir_camps_linia(self, installacio):
        """
        Gets the data of the line where the cel·la is placed

        :param installacio: Cel·la placement
        :return: Municipi, provincia, tensio of the line
        :rtype: dict
        """

        o = self.connection
        valor = installacio.split(',')
        model = valor[0]
        id_tram = int(valor[1])

        if model == "giscedata.at.suport":
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
            tensio = format_f(float(linia['tensio']) / 1000.0, decimals=3)

            if id_municipi and id_provincia:
                provincia = o.ResCountryState.read(id_provincia, ['code'])['code']
                municipi_dict = o.ResMunicipi.read(id_municipi, ['ine', 'dc'])
                municipi = '{0}{1}'.format(municipi_dict['ine'][-3:],
                                           municipi_dict['dc'])

            res = {
                'municipi': municipi,
                'provincia': provincia,
                'tensio': tensio
            }

        else:
            ct_data = o.GiscedataCts.read(id_tram, ['id_municipi',
                                                    'tensio_p'])
            municipi_id = ct_data['id_municipi'][0]
            tensio = format_f(float(ct_data['tensio_p']) / 1000.0, decimals=3)
            municipi_data = o.ResMunicipi.read(municipi_id, ['state',
                                                             'ine',
                                                             'dc'])
            id_provincia = municipi_data['state'][0]
            municipi = '{0}{1}'.format(municipi_data['ine'][-3:],
                                       municipi_data['dc'])
            provincia = o.ResCountryState.read(id_provincia, ['code'])['code']

            res = {
                'municipi': municipi,
                'provincia': provincia,
                'tensio': tensio
            }

        return res

    def consumer(self):
        """
        Function that generates each line of the file

        :return: None
        """

        o = self.connection
        fields_to_read = [
            'installacio', 'cini', 'propietari', 'name', 'tram_id', 'tensio',
            "node_id"
        ]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                cella = o.GiscedataCellesCella.read(
                    item, fields_to_read
                )
                o_fiabilitat = cella['name']
                o_cini = cella['cini']
                o_prop = int(cella['propietari'])
                o_tram = ""
                if cella.get("node_id"):
                    o_node = cella["node_id"][1]
                    geom = o.GiscegisNodes.read(cella["node_id"][0],["geom"])["geom"]
                    vertex = wkt.loads(geom).coords[0]

                dict_linia = self.obtenir_camps_linia(cella['installacio'])
                model, element_id = cella['installacio'].split(',')
                x = ''
                y = ''
                z = ''
                if model == "giscedata.cts":
                    ct_x_y = o.GiscedataCts.read(element_id, ["x", "y"])
                    vertex = False
                    o_tram = ""
                    if "node_id" not in cella:
                        o_node = self.get_node_ct(element_id)
                    point = (ct_x_y["x"], ct_x_y["y"])
                    p25830 = convert_srid(self.codi_r1, get_srid(o), point)
                    x = format_f(p25830[0], decimals=3)
                    y = format_f(p25830[1], decimals=3)
                else:
                    if not cella.get("node_id", False):
                        if not cella['tram_id']:
                            o_node, vertex, o_tram = self.get_node_vertex_tram(
                                o_fiabilitat)
                        else:
                            o_node, vertex = self.get_node_vertex(o_fiabilitat)
                    else:
                        if cella.get("tram_id"):
                            o_tram = "A{0}".format(o.GiscedataAtTram.read(
                                    cella['tram_id'][0], ['name']
                                )['name'])

                o_node = o_node.replace('*', '')
                o_municipi = dict_linia.get('municipi')
                o_provincia = dict_linia.get('provincia')
                if cella['tensio']:
                    tensio = o.GiscedataTensionsTensio.read(
                        cella['tensio'][0], ['tensio']
                    )
                    o_tensio = format_f(int(tensio['tensio'])/1000.0, decimals=3)
                else:
                    o_tensio = dict_linia.get('tensio')

                o_any = self.year
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(
                        self.codi_r1, get_srid(o), vertex)
                    x = format_f(res_srid[0], decimals=3)
                    y = format_f(res_srid[1], decimals=3)
                self.output_q.put([
                    o_node,         # NUDO
                    o_fiabilitat,   # ELEMENTO FIABILIDAD
                    o_tram,         # TRAMO
                    o_cini,         # CINI
                    x,              # X
                    y,              # Y
                    z,              # Z
                    o_municipi,     # MUNICIPIO
                    o_provincia,    # PROVINCIA
                    o_tensio,       # NIVEL TENSION
                    self.cod_dis,   # CODIGO DISTRIBUIDORA
                    o_prop,         # PROPIEDAD
                    o_any           # AÑO INFORMACION
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
