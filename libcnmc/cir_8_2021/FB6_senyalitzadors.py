#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARIO DE CNMC - Elementos de mejora de fiabilidad
"""
from datetime import datetime
import traceback
from libcnmc.utils import (format_f, convert_srid, get_srid, get_name_ti, format_f_6181, format_ccaa_code, get_ine,
                           adapt_diff, default_estado, calculate_estado)
from libcnmc.core import StopMultiprocessBased
from libcnmc.models import F7Res4666
from shapely import wkt

MODELO = {
    '1': 'I',
    '2': 'M',
    '3': 'D',
    '4': 'E'
}

class FB6Senyalitzadors(StopMultiprocessBased):
    """
    Class that generates the CT file of the 4666 for 'giscedata.at.detectors' model
    """
    def __init__(self, **kwargs):
        """
        Class constructor

        :param year: Year to generate
        :type year: int
        :param codi_r1:
        :type codi_r1:str
        """
        super(FB6Senyalitzadors, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1') or ''
        self.report_name = 'FB6 - Elements de millora de fiabilitat'
        self.base_object = 'Elements de millora de fiabilitat'
        self.cod_dis = 'R1-{}'.format(self.codi_r1[-3:])
        self.prefix_AT = kwargs.pop('prefix_at', 'A') or 'A'

    def get_sequence(self):
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-01-01' % self.year

        search_params_senyalitzadors = [
            '|', ('data_pm', '=', False),
            ('data_pm', '<', data_pm),
            '|', ('data_baixa', '>=', data_baixa),
            ('data_baixa', '=', False),
            ('cini', '=like', 'I27%'),
            '|', ('model', '!=', 'M'),
            ('data_baixa', '=', False)
        ]
        return self.connection.GiscedataAtDetectors.search(
                search_params_senyalitzadors, 0, 0, False,
                {'active_test': False}
            )

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

    def get_data_pm(self, data):
        """
        Function to get data_pm of data
        """
        data_pm = ''
        if data.get('data_pm', False):
            data_pm_ct = datetime.strptime(str(data['data_pm']),
                                           '%Y-%m-%d')
            data_pm = data_pm_ct.strftime('%d/%m/%Y')
        return data_pm

    def get_senyalitzador_tram_municipi_provincia(self, connection, senyalitzador_data):
        o_identificador_elemento = ''
        o_municipi = ''
        o_provincia = ''
        comunitat_codi = ''
        if senyalitzador_data.get('tram_id', False):
            # Si el senyalitzador té
            # tram associat, no es pot trobar la ubicació a través de la
            # línia AT
            o_provincia, o_municipi = self.get_ine(
                senyalitzador_data['municipi_id'])
            fun_ccaa = connection.ResComunitat_autonoma.get_ccaa_from_municipi
            id_comunitat = fun_ccaa(senyalitzador_data['municipi_id'])
            comunitat_vals = connection.ResComunitat_autonoma.read(
                id_comunitat[0], ['codi'])
            if comunitat_vals:
                comunitat_codi = comunitat_vals['codi']

            tram_data = connection.GiscedataAtTram.read(senyalitzador_data['tram_id'][0], ['name', 'id_regulatori'])
            if tram_data.get('id_regulatori', False):
                o_identificador_elemento = tram_data['id_regulatori']
            else:
                o_identificador_elemento = "{}{}".format(self.prefix_AT,
                                                         tram_data['name'])

        return o_identificador_elemento, o_municipi, o_provincia, comunitat_codi

    def get_obres_senyalitzador(self, connection, senyalitzador_data, fields_to_read_obra):
        # TODO: Pendent de dir per part de client si cal afegir associar obres als senyalitzadors
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

        return (tipo_inversion, im_ingenieria, im_materiales, im_obracivil, im_trabajos,
                im_construccion, subvenciones_europeas,
                subvenciones_nacionales, subvenciones_prtr,
                valor_auditado, valor_contabilidad,
                cuenta_contable, identificador_baja,
                financiado, avifauna)

    def fecha_baja_causa_baja_senyalitzador(self, connection, senyalitzador_data):
        identificador_baja = '' # TODO: Cal afegir la cerca de l'identificador de baixa dels senyalitzadors a través de les obres
        if senyalitzador_data['data_baixa']:
            data_pm_limit = '{0}-01-01'.format(self.year + 1)
            data_pm = self.get_data_pm(data=senyalitzador_data)
            if senyalitzador_data['data_baixa'] < data_pm_limit:
                tmp_date = datetime.strptime(
                    senyalitzador_data['data_baixa'], '%Y-%m-%d')
                fecha_baja = tmp_date.strftime('%d/%m/%Y')
                if int(fecha_baja.split("/")[2]) - int(
                        data_pm.split("/")[2]) >= 40:
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

        return fecha_baja, causa_baja

    def codigo_ccuu_senyalitzador(self, connection, senyalitzador_data):
        ti = ''
        if senyalitzador_data.get('tipus_instalacio_cnmc_id', False):
            id_ti = senyalitzador_data['tipus_instalacio_cnmc_id'][0]
            ti = connection.GiscedataTipusInstallacio.read(id_ti, ['name'])['name']
        return ti

    def get_senyalitzador_node(self, senyalitzador_data, o_fiabilitat):
        if senyalitzador_data.get('node_id'):
            o_node = senyalitzador_data['node_id'][1]
            vertex = wkt.loads(senyalitzador_data['geom']).coords[0]
        else:
            o_node, vertex = self.get_node_vertex(o_fiabilitat)
        o_node = o_node.replace('*', '')

        return o_node, vertex

    def get_tensio_senyalitzador(self, connection, senyalitzador_data):
        o_tensio = ''
        o_tensio_const = ''
        if senyalitzador_data.get('tram_id', False):
            tram_data = connection.GiscedataAtTram.read(
                senyalitzador_data['tram_id'][0], ['tensio']
            )
            o_tensio = tram_data.get('tensio', '')

        o_tensio_const = senyalitzador_data.get('tensio_construccio', '')

        return o_tensio, o_tensio_const

    def get_estado_formulario_senyalitzador(self, connection, senyalitzador_data, o_fiabilitat, o_identificador_elemento, ti, comunitat_codi, data_pm, fecha_baja, modelo, senyalitzador_obra=False):
        hist_obj = connection.model('circular.82021.historics.b6')
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
                senyalitzador_data['name'],
                senyalitzador_data['cini'],
                o_identificador_elemento,
                str(ti),
                comunitat_codi,
                data_pm,
                fecha_baja,
                0
            )
            estado = calculate_estado(
                fecha_baja, actual, entregada, senyalitzador_obra=senyalitzador_obra)
            if estado == '1' and not senyalitzador_obra:
                self.output_m.put("{} {}".format(
                    senyalitzador_data["name"], adapt_diff(actual.diff(entregada))))
        else:
            estado = default_estado(modelo, data_pm, int(self.year))

        return estado

    def get_senyalitzadors_data(self, item, connection):
        """
        Function to put senyalitzadors data in the output in flag 'incloure_senyalitzadors' is True
        """
        fields_to_read_senyalitzador = [
            'name', 'municipi_id', 'tram_id', 'data_pm', 'data_baixa', 'rao_baixa', 'tipus_instalacio_cnmc_id', 'tensio_construccio', 'punt_frontera', 'cini', 'model',
        ]
        senyalitzador_data = connection.GiscedataAtDetectors.read(
            item, fields_to_read_senyalitzador
        )
        data_pm = self.get_data_pm(data=senyalitzador_data)
        o_fiabilitat = senyalitzador_data['name']
        o_cini = senyalitzador_data['cini']
        o_prop = int(senyalitzador_data['propietari'])

        # MODEL
        if senyalitzador_data['model']:
            modelo = senyalitzador_data['model']
        else:
            modelo = ''

        # OBRES (TODO: Pendent de dir per part de client si cal afegir associar obres als senyalitzadors)
        (tipo_inversion, im_ingenieria, im_materiales, im_obracivil, im_trabajos,
         im_construccion, subvenciones_europeas,
         subvenciones_nacionales, subvenciones_prtr,
         valor_auditado, valor_contabilidad,
         cuenta_contable, identificador_baja,
         financiado, avifauna) = self.get_obres_senyalitzador(
            connection, senyalitzador_data, fields_to_read_obra=[]
        )

        # TRAM, MUNICIPI I PROVINCIA
        o_identificador_elemento, o_municipi, o_provincia, comunitat_codi = self.get_senyalitzador_tram_municipi_provincia(
            connection, senyalitzador_data
        )

        # FECHA BAJA, CAUSA_BAJA
        fecha_baja, causa_baja = self.fecha_baja_causa_baja_senyalitzador(
            connection, senyalitzador_data
        )

        # CODIGO CCUU
        ti = self.codigo_ccuu_senyalitzador(
            connection, senyalitzador_data
        )

        # NODE
        o_node, vertex = self.get_senyalitzador_node(
            senyalitzador_data, o_fiabilitat
        )

        # Tensio
        o_tensio, o_tensio_const = self.get_tensio_senyalitzador(
            connection, senyalitzador_data
        )

        punto_frontera = int(senyalitzador_data['punt_frontera'] == True)
        o_any = self.year
        x = ''
        y = ''
        if vertex:
            res_srid = convert_srid(get_srid(connection), vertex)
            x = format_f(res_srid[0], decimals=3)
            y = format_f(res_srid[1], decimals=3)

        # Estado
        estado = self.get_estado_formulario_senyalitzador(
            senyalitzador_data, o_fiabilitat, o_identificador_elemento,
            ti, comunitat_codi, data_pm, fecha_baja, modelo, senyalitzador_obra=False
        )

        if modelo == 'M':
            estado = ''
            data_pm = ''

        if causa_baja == '0':
            fecha_baja = ''

        if modelo == 'E' and estado == '2':
            tipo_inversion = '0'

        self.output_q.put([
            o_fiabilitat,  # ELEMENTO FIABILIDAD
            o_cini,  # CINI
            o_identificador_elemento,  # IDENTIFICADOR_ELEMENTO
            o_node,  # NUDO
            x,  # X
            y,  # Y
            '0,000',  # Z
            o_municipi,  # MUNICIPIO
            o_provincia,  # PROVINCIA
            comunitat_codi,  # CCAA
            str(ti),  # CCUU
            o_tensio,  # NIVEL TENSION EXPLOTACION
            o_tensio_const,  # TENSION CONST
            data_pm,  # FECHA_APS
            fecha_baja,  # FECHA_BAJA
            causa_baja,  # CAUSA_BAJA
            estado,  # ESTADO
            modelo,  # MODELO
            punto_frontera,  # PUNT_FRONTERA
            tipo_inversion,  # TIPO_INVERSION
            im_ingenieria,  # IM_TRAMITES
            im_construccion,  # IM_CONSTRUCCION
            im_trabajos,  # IM_TRABAJOS
            subvenciones_europeas,  # SUBVENCIONES_EUROPEAS
            subvenciones_nacionales,  # SUBVENCIONES_NACIONALES
            subvenciones_prtr,  # SUBVENCIONES_PRTR
            valor_auditado,  # VALOR_AUDITADO
            financiado,  # FINANCIADO
            cuenta_contable,  # CUENTA
            avifauna,  # AVIFAUNA
            identificador_baja,  # IDENTIFICADOR_BAJA
        ])

    def consumer(self):
        """
        Function that generates each line of the file

        :return: None
        """
        try:
            O = self.connection
            while True:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                self.get_senyalitzadors_data()
                self.input_q.task_done()
        except Exception:
            self.input_q.task_done()
            traceback.print_exc()
            if self.raven:
                self.raven.captureException()