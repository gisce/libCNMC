# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.utils import format_f, convert_srid, get_srid
from libcnmc.core import MultiprocessBased
from shapely import wkt


class F15(MultiprocessBased):
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

        super(F15, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'F15 - Cel·les'
        self.base_object = 'Cel·les'
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

    def get_node_vertex(self, element_name):
        """
        Return the node and the (X,Y) of the Cel·la retrieving this values from
        the Blocs models
        :param element_name: Name of the Cel·la
        :rtype element_name: str
        :return: The Node name and the (X,Y) of the Cel·la
        :rtype: (str or bool, (float, float) or bool)
        """
        o = self.connection
        node = ''
        vertex = False
        bloc_id = []
        model_ok = False
        if element_name:
            # Search on the diferent models
            models = [
                o.GiscegisBlocsInterruptorat,
                o.GiscegisBlocsFusiblesat,
                o.GiscegisBlocsSeccionadorat,
                o.GiscegisBlocsSeccionadorunifilar
            ]
            for model in models:
                bloc_id = model.search([('codi', '=', element_name)])
                if bloc_id:
                    model_ok = model
                    break
            if bloc_id:
                bloc = model_ok.read(
                    bloc_id[0], ['node', 'vertex']
                )
                if bloc.get('node', False):
                    node = bloc['node'][1]
                if bloc.get('vertex', False):
                    v = o.GiscegisVertex.read(bloc['vertex'][0], ['x', 'y'])
                    vertex = (v['x'], v['y'])

        return node, vertex

    def obtenir_camps_linia(self, installacio):
        """
        Gets the data of the line where the cel·la is placed

        :param installacio: Cel·la placement
        :return: Municipi, provincia, tensio of the line
        :rtype: dict
        """

        o = self.connection
        id_tram = int(installacio.split(',')[1])

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

        return res

    def consumer(self):
        """
        Function that generates each line of the file

        :return: None
        """

        o = self.connection
        fields_to_read = [
            'installacio', 'cini', 'propietari', 'name', 'tensio', 'node_id',
            'geom'
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
                if cella['installacio']:
                    tram_id = int(cella['installacio'].split(',')[1])
                    o_tram = "A{0}".format(
                        o.GiscedataAtTram.read(tram_id, ['name'])['name']
                    )
                if cella.get("node_id"):
                    o_node = cella["node_id"][1]
                    vertex = wkt.loads(cella["geom"]).coords[0]
                else:
                    o_node, vertex = self.get_node_vertex(o_fiabilitat)
                o_node = o_node.replace('*', '')

                dict_linia = self.obtenir_camps_linia(cella['installacio'])
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
                x = ''
                y = ''
                z = ''
                if vertex:
                    res_srid = convert_srid(get_srid(o), vertex)
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
