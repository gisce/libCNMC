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
        self.prefix_AT = kwargs.pop('prefix_at', 'A') or 'A'

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
            ('cini', '=like', 'I26%'),
            ('inventari', '=', 'fiabilitat'),
        ]
        # Excloure els registres que es troben de baixa i el model es 'M'
        search_params += [
            '|', ('model', '!=', 'M'), ('data_baixa', '=', False)
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

    def obtenir_camps_linia_at(self, linia_id):
        """
        Gets the data of the line where the cel·la is placed

        :param installacio: Cel·la placement
        :return: Municipi, provincia, tensio of the line
        :rtype: dict
        """

        o = self.connection
        tensio_obj = o.GiscedataTensionsTensio

        res = {
            'id_municipi': '',
            'tensio': '',
            'name': ''
        }

        fields_to_read = ['municipi', 'tensio_id', 'name']
        linia_data = o.GiscedataAtLinia.read(linia_id, fields_to_read)

        if linia_data.get('municipi', False):
            res['id_municipi'] = linia_data['municipi'][0]
        if linia_data.get('tensio_id', False):
            tensio_id = linia_data['tensio_id'][0]
            tensio_data = tensio_obj.read(tensio_id, ['tensio'])
            if tensio_data.get('tensio', False):
                res['tensio'] = format_f(float(tensio_data['tensio']) / 1000.0, decimals=3)
        if linia_data.get('name', False):
            res['name'] = linia_data['name'][0]

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
            'geom', 'tram_id', 'id', 'data_pm', 'data_baixa',
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
                cella_obra = ''
                obra_ti_pos_obj = O.GiscedataProjecteObraTiCelles
                obra_ti_ids = obra_ti_pos_obj.search([('element_ti_id', '=', cella['id'])])
                if obra_ti_ids:
                    for obra_ti_id in obra_ti_ids:
                        obra_id_data = obra_ti_pos_obj.read(obra_ti_id, ['obra_id'])
                        obra_id = obra_id_data['obra_id']
                        # Filtre d'obres finalitzades
                        data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                        if data_finalitzacio_data:
                            if data_finalitzacio_data.get('data_finalitzacio', False):
                                data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                                inici_any = '{}-01-01'.format(self.year)
                                fi_any = '{}-12-31'.format(self.year)
                                if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                    cella_obra = O.GiscedataProjecteObraTiCelles.read(obra_ti_id, fields_to_read_obra)
                        if cella_obra:
                            break

                tipo_inversion = ''
                # CAMPS OBRA
                if cella_obra != '':
                    tipo_inversion = cella_obra['tipo_inversion'] or ''
                    im_ingenieria = format_f_6181(cella_obra['im_ingenieria'] or 0.0, float_type='euro')
                    im_materiales = format_f_6181(cella_obra['im_materiales'] or 0.0, float_type='euro')
                    im_obracivil = format_f_6181(cella_obra['im_obracivil'] or 0.0, float_type='euro')
                    im_trabajos = format_f_6181(cella_obra['im_trabajos'] or 0.0, float_type='euro')
                    identificador_baja = ''
                    if cella_obra.get('identificador_baja', False):
                        cella_id = cella_obra['identificador_baja'][0]
                        cella_data = O.GiscedataCellesCella.read(cella_id, ['name', 'id_regulatori'])
                        if cella_data.get('id_regulatori', False):
                            identificador_baja = cella_data['id_regulatori']
                        else:
                            identificador_baja = cella_data['name']
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

                # TRAM, MUNICIPI I PROVINCIA
                o_identificador_elemento = ''
                id_municipi = ''
                linia_data = {}
                ct_data = {}
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
                        suport_data = O.GiscedataAtSuport.read(inst_id, ['linies_at_ids'])
                        if suport_data.get('linies_at_ids', False):
                            linia_id = suport_data['linies_at_ids'][0]
                            linia_data = self.obtenir_camps_linia_at(linia_id)

                    elif inst_model == 'giscedata.cts':
                        ct_data = model_obj.read(inst_id, ['name', 'id_regulatori', 'id_municipi'])
                        if ct_data.get('id_regulatori', False):
                            o_identificador_elemento = ct_data['id_regulatori']
                        else:
                            o_identificador_elemento = ct_data['name']

                else:
                    o_identificador_elemento = self.get_node_vertex_tram(o_fiabilitat)

                o_municipi = ''
                o_provincia = ''
                comunitat_codi = ''
                if linia_data.get('id_municipi', False):
                    id_municipi = linia_data['id_municipi']
                    o_provincia, o_municipi = self.get_ine(id_municipi)
                elif ct_data.get('id_municipi'):
                    id_municipi = ct_data['id_municipi'][0]
                    o_provincia, o_municipi = self.get_ine(id_municipi)

                # funció per trobar la ccaa desde el municipi
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                if id_municipi:
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat_codi = comunitat_vals['codi']

                #FECHA BAJA, CAUSA_BAJA
                if cella['data_baixa']:
                    if cella['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            cella['data_baixa'], '%Y-%m-%d')
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
                ti = ''
                if cella.get('tipus_instalacio_cnmc_id', False):
                    id_ti = cella['tipus_instalacio_cnmc_id'][0]
                    ti = O.GiscedataTipusInstallacio.read(id_ti, ['name'])['name']

                #NODE
                if cella.get('node_id'):
                    o_node = cella['node_id'][1]
                    vertex = wkt.loads(cella['geom']).coords[0]
                else:
                    o_node, vertex = self.get_node_vertex(o_fiabilitat)
                o_node = o_node.replace('*', '')

                if cella['tensio']:
                    tensio = O.GiscedataTensionsTensio.read(
                        cella['tensio'][0], ['tensio']
                    )
                    o_tensio = format_f(int(tensio['tensio'])/1000.0, decimals=3)
                else:
                    o_tensio = linia_data.get('tensio', '0,000')

                # TENSIO_CONST
                o_tensio_const = ''
                if cella.get('tensio_const', False):
                    o_tensio_const = format_f(float(cella['tensio_const'][1]) / 1000.0, decimals=3) or ''

                if o_tensio_const == o_tensio:
                    o_tensio_const = ''

                punto_frontera = int(cella['punt_frontera'] == True)
                o_any = self.year
                x = ''
                y = ''
                if vertex:
                    res_srid = convert_srid(get_srid(O), vertex)
                    x = format_f(res_srid[0], decimals=3)
                    y = format_f(res_srid[1], decimals=3)

                # ESTADO
                hist_obj = O.model('circular.82021.historics.b6')
                hist_ids = hist_obj.search([
                    ('identificador_em', '=', o_fiabilitat),
                    ('year', '=', self.year - 1)
                ])
                if hist_ids:
                    hist = hist_obj.read(hist_ids[0], [
                        'cini', 'codigo_ccuu', 'fecha_aps'
                    ])
                    entregada = F7Res4666(
                        cini=hist['cini'],
                        codigo_ccuu=hist['codigo_ccuu'],
                        fecha_aps=hist['fecha_aps']
                    )
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
                        if cella_obra:
                            estado = '1'
                    else:
                        self.output_m.put("{} {}".format(cella["name"], adapt_diff(actual.diff(entregada))))
                        estado = '1'
                else:
                    estado = '2'

                if modelo == 'M':
                    estado = ''
                    data_pm = ''

                if causa_baja in [1, 3]:
                    tipo_inversion = ''

                # L'any 2022 no es declaren subvencions PRTR
                subvenciones_prtr = ''

                if causa_baja == '0':
                    fecha_baja = ''

                if modelo == 'E':
                    tipo_inversion = '0'
                    estado = '1'

                # ESTADO no pot ser 2 si FECHA_APS < 2022
                if not modelo == 'M':
                    if data_pm:
                        fecha_aps_year = int(data_pm.split('/')[2])
                        if estado == '2' and fecha_aps_year != int(self.year):
                            estado = '1'
                        elif fecha_aps_year == int(self.year):
                            estado = '2'
                    else:
                        estado = '1'

                self.output_q.put([
                    o_fiabilitat,   # ELEMENTO FIABILIDAD
                    o_cini,  # CINI
                    o_identificador_elemento,  #IDENTIFICADOR_ELEMENTO
                    o_node,  # NUDO
                    x,              # X
                    y,              # Y
                    '0,000',              # Z
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
