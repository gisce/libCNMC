#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from datetime import datetime
import traceback
from libcnmc.utils import format_f, convert_srid, get_srid, get_name_ti, format_f_6181, format_ccaa_code, adapt_diff
from libcnmc.core import MultiprocessBased
from libcnmc.models import F8Res4666
from shapely import wkt



class FB6(MultiprocessBased):

    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        """
        Class constructor

        :param year: Year to generate
        :type year: int
        :param codi_r1:
        :type codi_r1:str
        """

        super(FB6, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'FB6 - Elements de millora de fiabilitat'
        self.base_object = 'Elements de millora de fiabilitat'
        self.cod_dis = 'R1-{}'.format(self.codi_r1[-3:])
        self.compare_field = "4666_entregada"


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

    def get_node_vertex_tram(self, element_name):
        o = self.connection
        tram_name = ""
        if element_name:
            # Search on the diferent models
            models = [
                o.GiscegisBlocsInterruptorat, o.GiscegisBlocsFusiblesat,
                o.GiscegisBlocsSeccionadorat, o.GiscegisBlocsSeccionadorunifilar
            ]
            for model in models:
                bloc_id = model.search([('codi', '=', element_name)])
                if bloc_id:
                    bloc_id = bloc_id[0]
                    bloc = model.read(bloc_id, ['node', 'vertex'])
                    v = o.GiscegisVertex.read(bloc['vertex'][0], ['id'])
                    polver_ids = o.GiscegisPolylineVertex.search(
                        [('vertex', '=', v['id'])]
                    )
                    if polver_ids:
                        poly_id = o.GiscegisPolyline.search(
                            [('vertex_ids', 'in', polver_ids[0])]
                        )[0]
                        edge_id = o.GiscegisEdge.search(
                            [('polyline', '=', poly_id)]
                        )[0]
                        linktemplate = o.GiscegisEdge.read(
                            edge_id, ['id_linktemplate']
                        )['id_linktemplate']
                        tram_id = o.GiscedataAtTram.search(
                            [('name', '=', linktemplate)]
                        )
                        tram_name = o.GiscedataAtTram.read(
                            tram_id, ['name']
                        )[0]['name']
                        return "A{0}".format(tram_name)
        return ""

    def consumer(self):
        """
        Function that generates each line of the file

        :return: None
        """

        O = self.connection

        def get_inst_name(element_id):
            vals = self.connection.GiscedataCellesCella.read(
                element_id, ['name'])
            return vals['name']

        fields_to_read = [
            'installacio', 'cini', 'propietari', 'name', 'tensio', 'node_id', 'perc_financament',
            'tipus_instalacio_cnmc_id',
            'geom', 'tram_id', 'id', 'data_pm', 'data_baixa', self.compare_field,
        ]

        fields_to_read_obra = [
            'name',
            'cini',
            'tipo_inversion',
            'ccuu',
            'codigo_ccaa',
            'nivel_tension_explotacion',
            'elemento_act',
            'financiado',
            'fecha_aps',
            'fecha_baja',
            'causa_baja',
            'im_ingenieria',
            'im_materiales',
            'im_obracivil',
            'im_trabajos',
            'subvenciones_europeas',
            'subvenciones_nacionales',
            'valor_auditado',
            'valor_contabilidad',
            'cuenta_contable',
            'porcentaje_modificacion',
            'motivacion',
            'obra_id',
            'identificador_baja',
        ]

        data_pm_limit = '{0}-01-01'.format(self.year + 1)

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                cella = O.GiscedataCellesCella.read(
                    item, fields_to_read
                )

                obra_id = O.GiscedataProjecteObraTiCelles.search([('element_ti_id', '=', cella['id'])])

                if obra_id:
                    linia = O.GiscedataProjecteObraTiCelles.read(obra_id, fields_to_read_obra)[0]
                else:
                    linia = ''

                if linia != '':
                    tipo_inversion = (linia['tipo_inversion'] or '0') if not linia['fecha_baja'] else '1'
                    im_ingenieria = format_f_6181(linia['im_ingenieria'] or 0.0, float_type='euro')
                    im_materiales = format_f_6181(linia['im_materiales'] or 0.0, float_type='euro')
                    im_obracivil = format_f_6181(linia['im_obracivil'] or 0.0, float_type='euro')
                    im_trabajos = format_f_6181(linia['im_trabajos'] or 0.0, float_type='euro')
                    identificador_baja = (
                        get_inst_name(linia['identificador_baja'][0]) if linia['identificador_baja'] else ''
                    )
                    im_construccion = str(
                        float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", "."))
                    ).replace(".", ",")

                    subvenciones_europeas = format_f_6181(linia['subvenciones_europeas'] or 0.0, float_type='euro')
                    subvenciones_nacionales = format_f_6181(linia['subvenciones_nacionales'] or 0.0, float_type='euro')
                    valor_auditado = format_f_6181(linia['valor_auditado'] or 0.0, float_type='euro')
                    valor_contabilidad = format_f_6181(linia['valor_contabilidad'] or 0.0, float_type='euro')
                    cuenta_contable = linia['cuenta_contable']
                    financiado =format_f(
                        100.0 - linia.get('financiado', 0.0), 2
                    )

                else:
                    tipo_inversion = ''
                    ccuu = ''
                    ccaa = ''
                    im_ingenieria = ''
                    im_materiales = ''
                    im_obracivil = ''
                    im_trabajos = ''
                    im_construccion = ''
                    subvenciones_europeas = ''
                    subvenciones_nacionales = ''
                    valor_auditado = ''
                    valor_contabilidad = ''
                    cuenta_contable = ''
                    identificador_baja = ''
                    financiado = ''

                o_fiabilitat = cella['name']
                o_cini = cella['cini']
                o_prop = int(cella['propietari'])
                o_tram = ""
                if cella['tram_id']:
                    o_tram = "A{0}".format(
                        O.GiscedataAtTram.read(
                            cella['tram_id'][0], ['name']
                        )['name']
                    )
                else:
                    o_tram = self.get_node_vertex_tram(o_fiabilitat)

                data_pm = ''

                if cella['data_pm']:
                    data_pm_ct = datetime.strptime(str(cella['data_pm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_ct.strftime('%d/%m/%Y')

                if cella['data_baixa']:
                    if cella['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            cella['data_baixa'], '%Y-%m-%d %H:%M:%S')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')

                        if int(data_pm.split("/")[2]) - int(fecha_baja.split("/")[2]) >= 40:
                            if identificador_baja != '':
                                causa_baja = 1
                            else:
                                causa_baja = 2
                    else:
                        fecha_baja = ''
                        causa_baja = 0;
                else:
                    fecha_baja = ''
                    causa_baja = 0;

                id_ti = cella['tipus_instalacio_cnmc_id'][0]
                ti = O.GiscedataTipusInstallacio.read(
                    id_ti,
                    ['name'])['name']

                if cella[self.compare_field]:
                    last_data = cella[self.compare_field]
                    #entregada = F8Res4666(**last_data)

                    #actual = F8Res4666(
                    #    cella['name'],
                    #    cella['cini'],
                    #    cella['descripcio'],
                    #    ti,
                    #    format_f(
                    #        100.0 - cella.get('perc_financament', 0.0), 2
                    #    ),
                    #    data_pm,
                    #    fecha_baja,
                    #    0
                    #)
                    #if entregada == actual and fecha_baja == '':
                    #    estado = '0'
                    #else:
                    #    self.output_m.put("{} {}".format(cella["name"], adapt_diff(actual.diff(entregada))))
                    #    estado = '1'
                else:
                    if cella['data_pm']:
                        if cella['data_pm'][:4] != str(self.year):
                            self.output_m.put("Identificador:{} No estava en el fitxer carregat al any n-1 i la data de PM es diferent al any actual".format(cella["name"]))
                            estado = '1'
                        else:
                            estado = '2'
                    else:
                        self.output_m.put("Identificador:{} No estava en el fitxer carregat al any n-1".format(cella["name"]))
                        estado = '1'

                if cella.get("node_id"):
                    o_node = cella["node_id"][1]
                    vertex = wkt.loads(cella["geom"]).coords[0]
                else:
                    o_node, vertex = self.get_node_vertex(o_fiabilitat)
                o_node = o_node.replace('*', '')

                dict_linia = self.obtenir_camps_linia(cella['installacio'])
                o_municipi = dict_linia.get('municipi')
                o_provincia = dict_linia.get('provincia')

                # funció per trobar la ccaa desde el municipi
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                if o_municipi:
                    id_municipi = o_municipi
                else:
                    id_municipi = get_id_municipi_from_company(O)

                if id_municipi:
                    print("id_muni")
                    print(id_municipi)
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat_codi = comunitat_vals['codi']

                if cella['tensio']:
                    tensio = O.GiscedataTensionsTensio.read(
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
                    res_srid = convert_srid(get_srid(O), vertex)
                    x = format_f(res_srid[0], decimals=3)
                    y = format_f(res_srid[1], decimals=3)
                self.output_q.put([
                    o_fiabilitat,   # ELEMENTO FIABILIDAD
                    o_cini,  # CINI
                    o_tram,  #IDENTIFICADOR_ELEMENTO
                    o_node,  # NUDO
                    x,              # X
                    y,              # Y
                    z,              # Z
                    o_municipi,     # MUNICIPIO
                    o_provincia,    # PROVINCIA
                    comunitat_codi,     #CCAA
                    str(ti),     #CCUU
                    o_tensio,       # NIVEL TENSION EXPLOTACION
                    '',             # TENSION CONST
                    data_pm,        #FECHA_APS
                    fecha_baja,     #FECHA_BAJA
                    causa_baja,     #CAUSA_BAJA
                    estado,     #ESTADO
                    #MODELO
                    #PUNT_FRONTERA
                    tipo_inversion,     #TIPO_INVERSION
                    im_ingenieria,    #IM_TRAMITES
                    im_construccion,    #IM_CONSTRUCCION
                    im_trabajos,    #IM_TRABAJOS
                    subvenciones_europeas,      #SUBVENCIONES_EUROPEAS
                    subvenciones_nacionales,     #SUBVENCIONES_NACIONALES
                    #SUBVENCIONES_PRTR
                    valor_auditado,    #VALOR_AUDITADO
                    financiado,                 #FINANCIADO
                    cuenta_contable,    #CUENTA
                    #AVIFAUNA
                    identificador_baja,     #IDENTIFICADOR_BAJA
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
