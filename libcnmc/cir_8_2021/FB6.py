#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARIO DE CNMC - Elementos de mejora de fiabilidad
"""
from datetime import datetime
import traceback
from libcnmc.utils import (format_f, convert_srid, get_srid, get_name_ti, format_f_6181, format_ccaa_code, get_ine,
                           adapt_diff)
from libcnmc.core import StopMultiprocessBased
from libcnmc.models import F7Res4666
from shapely import wkt

MODELO = {
    '1': 'I',
    '2': 'M',
    '3': 'D',
    '4': 'E'
}

class FB6(StopMultiprocessBased):

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
        self.codi_r1 = kwargs.pop('codi_r1') or ''
        self.report_name = 'FB6 - Elements de millora de fiabilitat'
        self.base_object = 'Elements de millora de fiabilitat'
        self.cod_dis = 'R1-{}'.format(self.codi_r1[-3:])
        self.compare_field = "4666_entregada"
        self.prefix_AT = kwargs.pop('prefix_at', 'A') or 'A'
        self.compare_field = '4666_entregada'

    def get_sequence(self):
        """
                Generates the sequence of ids to pass to the consume function
                :return: List of ids to generate the
                :rtype: list(int)
                """
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-01-01' % self.year

        search_params = [('criteri_regulatori', '!=', 'excloure'),
                         '|', ('data_pm', '=', False),
                              ('data_pm', '<', data_pm),
                         '|', ('data_baixa', '>=', data_baixa),
                              ('data_baixa', '=', False)]

        search_params += [
            ("tipus_element.codi_cnmc", "!=", "T"),
            ('inventari', '=', 'fiabilitat'),
        ]

        return self.connection.GiscedataCellesCella.search(search_params, 0, 0, False, {'active_test': False})

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

    def get_ine(self, municipi_id):
        """
        Returns the INE code of the given municipi
        :param municipi_id: Id of the municipi
        :type municipi_id: int
        :return: state, ine municipi
        :rtype:tuple
        """
        O = self.connection
        muni = O.ResMunicipi.read(municipi_id, ['ine'])
        return get_ine(O, muni['ine'])

    def obtenir_camps_linia_at(self, installacio):
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
            'municipi', 'provincia', 'tensio', 'name'
        ]
        linia = o.GiscedataAtLinia.read(int(linia_id[0]), fields_to_read)
        municipi = ''
        id_municipi = linia['municipi'][0]
        name = linia['name']
        tensio = format_f(float(linia['tensio']) / 1000.0, decimals=3)

        res = {
            'id_municipi': id_municipi,
            'tensio': tensio,
            'name': name
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
                        return "{}{}".format(self.prefix_AT, tram_name)
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
            'tipus_instalacio_cnmc_id', 'punt_frontera', 'tensio_const', 'model',
            'geom', 'tram_id', 'id', 'data_pm', 'data_baixa', self.compare_field,
        ]
        fields_to_read_obra = [
            'name', 'cini', 'tipo_inversion', 'ccuu', 'codigo_ccaa', 'nivel_tension_explotacion', 'elemento_act',
            'financiado', 'fecha_aps', 'fecha_baja', 'causa_baja', 'im_ingenieria', 'im_materiales', 'im_obracivil',
            'im_trabajos', 'subvenciones_europeas', 'subvenciones_nacionales', 'subvenciones_prtr', 'avifauna',
            'valor_auditado', 'valor_contabilidad', 'cuenta_contable', 'porcentaje_modificacion', 'motivacion',
            'obra_id', 'identificador_baja',
        ]

        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                cella = O.GiscedataCellesCella.read(
                    item, fields_to_read
                )

                # MODEL
                if cella['model']:
                    modelo = cella['model']
                else:
                    modelo = ''

                # FECHA_APS
                data_pm = ''
                if cella['data_pm']:
                    data_pm_ct = datetime.strptime(str(cella['data_pm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_ct.strftime('%d/%m/%Y')

                # OBRES

                obra_ti_pos_obj = O.GiscedataProjecteObraTiCelles
                obra_ti_pos_id = obra_ti_pos_obj.search([('element_ti_id', '=', cella['id'])])
                if obra_ti_pos_id:
                    obra_id_data = obra_ti_pos_obj.read(obra_ti_pos_id[0], ['obra_id'])
                else:
                    obra_id_data = {}

                # Filtre d'obres finalitzades
                cella_obra = ''
                if obra_id_data.get('obra_id', False):
                    obra_id = obra_id_data['obra_id']
                    data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                    if data_finalitzacio_data:
                        if data_finalitzacio_data.get('data_finalitzacio', False):
                            data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                            inici_any = '{}-01-01'.format(self.year)
                            fi_any = '{}-12-31'.format(self.year)
                            if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                cella_obra = O.GiscedataProjecteObraTiCelles.read(obra_ti_pos_id[0],
                                                                                  fields_to_read_obra)
                else:
                    cella_obra = ''

                # CAMPS OBRA
                if cella_obra != '':
                    tipo_inversion = (cella_obra['tipo_inversion'] or '0') if not cella_obra['fecha_baja'] else '1'
                    im_ingenieria = format_f_6181(cella_obra['im_ingenieria'] or 0.0, float_type='euro')
                    im_materiales = format_f_6181(cella_obra['im_materiales'] or 0.0, float_type='euro')
                    im_obracivil = format_f_6181(cella_obra['im_obracivil'] or 0.0, float_type='euro')
                    im_trabajos = format_f_6181(cella_obra['im_trabajos'] or 0.0, float_type='euro')
                    identificador_baja = (
                        get_inst_name(cella_obra['identificador_baja'][0]) if cella_obra['identificador_baja'] else ''
                    )
                    im_construccion = str(format_f(
                        float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", "."))
                    , 2)).replace(".", ",")
                    subvenciones_europeas = format_f_6181(cella_obra['subvenciones_europeas'] or 0.0, float_type='euro')
                    subvenciones_nacionales = format_f_6181(cella_obra['subvenciones_nacionales'] or 0.0, float_type='euro')
                    subvenciones_prtr = format_f_6181(cella_obra['subvenciones_prtr'] or 0.0, float_type='euro')
                    valor_auditado = format_f_6181(cella_obra['valor_auditado'] or 0.0, float_type='euro')
                    valor_contabilidad = format_f_6181(cella_obra['valor_contabilidad'] or 0.0, float_type='euro')
                    cuenta_contable = cella_obra['cuenta_contable'] or ''
                    avifauna = int(cella_obra['avifauna'] == True)
                    financiado = format_f(cella_obra.get('financiado') or 0.0, 2)
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
                    avifauna = ''
                    subvenciones_prtr = ''

                o_fiabilitat = cella['name']
                o_cini = cella['cini']
                o_prop = int(cella['propietari'])

                #TRAM
                o_identificador_elemento = ""
                if cella.get('installacio', False):
                    installacio_data = cella['installacio']
                    inst_model = installacio_data.split(',')[0]
                    inst_id = int(installacio_data.split(',')[1])
                    model_obj = O.model(inst_model)

                    if inst_model == 'giscedata.at.suport':
                        if cella.get('tram_id', False):
                            tram_data = O.GiscedataAtTram.read(cella['tram_id'][0], ['name', 'id_regulatori'])
                            if tram_data.get('id_regulatori', False):
                                o_identificador_elemento = tram_data['id_regulatori']
                            else:
                                o_identificador_elemento = "{}{}".format(self.prefix_AT, tram_data['name'])

                    if inst_model == 'giscedata.cts':
                        ct_data = model_obj.read(inst_id, ['name', 'id_regulatori'])
                        if ct_data.get('id_regulatori', False):
                            o_identificador_elemento = ct_data['id_regulatori']
                        else:
                            o_identificador_elemento = ct_data['name']
                else:
                    o_identificador_elemento = self.get_node_vertex_tram(o_fiabilitat)

                #FECHA BAJA, CAUSA_BAJA
                if cella['data_baixa']:
                    if cella['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            cella['data_baixa'], '%Y-%m-%d %H:%M:%S')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')
                        if int(fecha_baja.split("/")[2]) - int(data_pm.split("/")[2]) >= 40:
                            if identificador_baja != '':
                                causa_baja = 1
                            else:
                                causa_baja = 2
                        else:
                            causa_baja = 3
                    else:
                        fecha_baja = ''
                        causa_baja = 0
                else:
                    fecha_baja = ''
                    causa_baja = 0

                #CODIGO CCUU
                id_ti = cella['tipus_instalacio_cnmc_id'][0]
                ti = O.GiscedataTipusInstallacio.read(
                    id_ti,
                    ['name'])['name']

                #NODE
                if cella.get('node_id'):
                    o_node = cella['node_id'][1]
                    vertex = wkt.loads(cella['geom']).coords[0]
                else:
                    o_node, vertex = self.get_node_vertex(o_fiabilitat)
                o_node = o_node.replace('*', '')

                element =  cella['installacio'].split(',')[0]
                dict_linia = self.obtenir_camps_linia_at(cella['installacio'])

                # MUNICIPI I PROVINCIA
                o_municipi = ''
                o_provincia = ''
                if dict_linia.get('id_municipi', False):
                    id_municipi = dict_linia['id_municipi']
                    o_provincia, o_municipi = self.get_ine(id_municipi)

                o_name = dict_linia.get('name')

                # funció per trobar la ccaa desde el municipi
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                if id_municipi:
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

                if cella['tensio_const']:
                    o_tensio_const = cella['tensio_const']
                else:
                    o_tensio_const = ''

                punto_frontera = int(cella['punt_frontera'] == True)
                o_any = self.year
                x = ''
                y = ''
                z = ''
                if vertex:
                    res_srid = convert_srid(get_srid(O), vertex)
                    x = format_f(res_srid[0], decimals=3)
                    y = format_f(res_srid[1], decimals=3)

                # ESTADO
                if cella[self.compare_field] and str(self.year + 1) not in str(data_pm):
                    last_data = cella[self.compare_field]
                    entregada = F7Res4666(**last_data)
                    actual = F7Res4666(
                        cella['name'],
                        cella['cini'],
                        o_identificador_elemento,
                        str(ti),
                        comunitat_codi,
                        data_pm,
                        fecha_baja,
                        0
                    )
                    if entregada == actual and fecha_baja == '':
                        estado = '0'
                    else:
                        self.output_m.put("{} {}".format(cella["name"], adapt_diff(actual.diff(entregada))))
                        estado = '1'
                else:
                    if cella['data_pm']:
                        if cella['data_pm'][:4] != str(self.year):
                            self.output_m.put(
                                "Identificador:{} No estava en el fitxer carregat al any n-1 i la data de PM es diferent al any actual".format(
                                    cella["name"]))
                            estado = '1'
                        else:
                            estado = '2'
                    else:
                        self.output_m.put(
                            "Identificador:{} No estava en el fitxer carregat al any n-1".format(cella["name"]))
                        estado = '1'

                if cella_obra:
                    estado = '1'

                if modelo == 'M':
                    estado = ''
                    fecha_aps = ''

                self.output_q.put([
                    o_fiabilitat,   # ELEMENTO FIABILIDAD
                    o_cini,  # CINI
                    o_identificador_elemento,  #IDENTIFICADOR_ELEMENTO
                    o_node,  # NUDO
                    x,              # X
                    y,              # Y
                    z,              # Z
                    o_municipi,     # MUNICIPIO
                    o_provincia,    # PROVINCIA
                    comunitat_codi,     #CCAA
                    str(ti),     #CCUU
                    o_tensio,       # NIVEL TENSION EXPLOTACION
                    o_tensio_const,             # TENSION CONST
                    data_pm,        #FECHA_APS
                    fecha_baja,     #FECHA_BAJA
                    causa_baja,     #CAUSA_BAJA
                    estado,     #ESTADO
                    modelo,      #MODELO
                    punto_frontera,  #PUNT_FRONTERA
                    tipo_inversion,     #TIPO_INVERSION
                    im_ingenieria,    #IM_TRAMITES
                    im_construccion,    #IM_CONSTRUCCION
                    im_trabajos,    #IM_TRABAJOS
                    subvenciones_europeas,      #SUBVENCIONES_EUROPEAS
                    subvenciones_nacionales,     #SUBVENCIONES_NACIONALES
                    subvenciones_prtr,  #SUBVENCIONES_PRTR
                    valor_auditado,    #VALOR_AUDITADO
                    financiado,                 #FINANCIADO
                    cuenta_contable,    #CUENTA
                    avifauna,   #AVIFAUNA
                    identificador_baja,     #IDENTIFICADOR_BAJA
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
