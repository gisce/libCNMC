#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import (format_f, tallar_text, format_f_6181, get_codi_actuacio, convert_spanish_date,
                           get_forced_elements, adapt_diff, fetch_tensions_norm, calculate_estado, default_estado)
from libcnmc.models import F1Res4666,F2Res4666


class FB1(StopMultiprocessBased):
    """
    Class that generates the FB1(1) file of  4666
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(FB1, self).__init__(**kwargs)
        self.extended = kwargs.get("extended", False)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Linies AT'
        self.report_name = 'CNMC INVENTARI AT'
        self.layer = 'LBT\_%'
        self.embarrats = kwargs.pop('embarrats', False)
        self.linia_tram_include = {}
        self.forced_ids = {}
        self.prefix_AT = kwargs.pop('prefix_at', 'A') or 'A'
        self.prefix_BT = kwargs.pop('prefix_bt', '')
        self.dividir = kwargs.pop('div', False)
        self.tensions = fetch_tensions_norm(self.connection)

        O = self.connection

        id_res_like = self.connection.ResConfig.search(
            [('name', '=', 'giscegis_btlike_layer')])
        if id_res_like:
            self.layer = self.connection.ResConfig.read(
                id_res_like, ['value'])[0]['value']

        trafos_fields = O.GiscedataTransformadorTrafo.fields_get().keys()
        if 'node_id' in trafos_fields:
            ids_red = O.GiscedataTransformadorTrafo.search(
                [('reductor', '=', True)]
            )
            data_nodes = O.GiscedataTransformadorTrafo.read(
                ids_red, ["node_id"]
            )
            self.nodes_red = [
                nod["node_id"][1] for nod in data_nodes if nod["node_id"]
            ]
        else:
            ids_red = O.GiscegisBlocsTransformadorsReductors.search([])
            data_nodes = O.GiscegisBlocsTransformadorsReductors.read(
                ids_red, ["node"]
            )
            self.nodes_red = [
                nod["node"][1] for nod in data_nodes if nod["node"]
            ]

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer

        :return: List of ids
        :rtype: list(int)
        """
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-01-01' % self.year

        # AT
        search_params = [('criteri_regulatori', '!=', 'excloure'),
                         '|', ('data_pm', '=', False),
                              ('data_pm', '<', data_pm),
                         '|', ('data_baixa', '>=', data_baixa),
                              ('data_baixa', '=', False)]

        if not self.embarrats:
            search_params += [
                ('cable.tipus.codi', '!=', 'E')
            ]

        # Revisem que si està de baixa ha de tenir la data informada
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        # Excloure els registres que es troben de baixa i el model es 'M'
        search_params += [
            '|', ('model', '!=', 'M'), ('data_baixa', '=', False)
        ]

        obj_lat = self.connection.GiscedataAtTram
        ids = obj_lat.search(
            search_params, 0, 0, False, {'active_test': False})
        at_ids = list(set(ids))

        for elem in range(0, len(at_ids)):
            at_ids[elem] = 'at.{}'.format(at_ids[elem])

        # BT
        search_params = [('criteri_regulatori', '!=', 'excloure'),
                         '|', ('data_pm', '=', False),
                              ('data_pm', '<', data_pm),
                         '|', ('data_baixa', '>=', data_baixa),
                              ('data_baixa', '=', False)]

        if not self.embarrats:
            search_params += [
                ('cable.tipus.codi', '!=', 'E')
            ]

        # Revisem que si està de baixa ha de tenir la data informada
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        # Excloure els registres que es troben de baixa i el model es 'M'
        search_params += [
            '|', ('model', '!=', 'M'), ('data_baixa', '=', False)
        ]

        obj_lbt = self.connection.GiscedataBtElement
        ids = obj_lbt.search(
            search_params, 0, 0, False, {'active_test': False})
        bt_ids = list(set(ids))

        for elem in range(0, len(bt_ids)):
            bt_ids[elem] = 'bt.{}'.format(bt_ids[elem])

        at_bt_ids = at_ids + bt_ids
        return at_bt_ids

    def consumer(self):
        """
        Method that generates the csv file
        :return: List of arrays
        """
        O = self.connection

        fields_to_read = [
            'baixa', 'data_pm', 'data_industria', 'coeficient', 'cini', 'propietari', 'tensio_max_disseny_id', 'name',
            'origen', 'final', 'perc_financament', 'longitud_cad', 'cable', 'linia', 'model', 'punt_frontera',
            'tipus_instalacio_cnmc_id', 'data_baixa', 'baixa', 'longitud_cad', 'data_pm', 'circuits',
            'id_regulatori', 'municipi', 'operacion',
        ]
        data_pm_limit = '{0}-01-01'.format(self.year + 1)

        def get_inst_name(element_id):
            vals = self.connection.GiscedataCts.read(
                element_id[0], ['name'])
            return vals['name']

        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                model = item.split('.')[0]
                item = int(item.split('.')[1])

                if model == 'at':

                    tram = O.GiscedataAtTram.read(item, fields_to_read)

                    # Coeficient per ajustar longituds de trams
                    coeficient = tram.get('coeficient', 1.0)
                    if tram.get('tipus_instalacio_cnmc_id', False):
                        id_ti = tram.get('tipus_instalacio_cnmc_id')[0]
                        codi_ccuu = O.GiscedataTipusInstallacio.read(
                            id_ti,
                            ['name'])['name']
                    else:
                        codi_ccuu = ''

                    # Comunitat
                    comunitat = ''
                    fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                    if tram.get('municipi', False):
                        municipi_id = tram['municipi'][0]
                        id_comunitat = fun_ccaa(municipi_id)
                        comunitat_vals = O.ResComunitat_autonoma.read(id_comunitat[0], ['codi'])
                        if comunitat_vals:
                            comunitat = comunitat_vals['codi']

                    # Propietari
                    if tram.get('propietari', False):
                        if tram['propietari']:
                            propietari = '1'
                        else:
                            propietari = '0'
                    else:
                        propietari = '0'

                    # Tensión_explotación
                    tension_explotacion = ''
                    if tram.get('linia', False):
                        linia_id, linia_name = tram['linia']
                        if linia_id:
                            tensio_data = O.GiscedataAtLinia.read(linia_id, ['tensio_id'])
                            if tensio_data.get('tensio_id', False):
                                tension_explotacion = self.tensions.get(tensio_data['tensio_id'][0], '')

                    # Tensión_construcción
                    tension_construccion = ''
                    if tram.get('tensio_max_disseny_id', False):
                        tension_construccion = self.tensions.get(tram['tensio_max_disseny_id'][0], '')
                        if str(tension_construccion) == str(tension_explotacion):
                            tension_construccion = ''

                    # Longitud
                    if 'longitud_cad' in tram:
                        if self.dividir:
                            long_tmp = tram['longitud_cad']/tram.get(
                                'circuits', 1
                            ) or 1
                            longitud = round(
                                long_tmp * coeficient/1000.0, 3
                            ) or 0.001
                        else:
                            longitud = round(
                                tram['longitud_cad'] * coeficient/1000.0, 3
                            ) or 0.001
                    else:
                        longitud = 0

                    # Resistencia, Reactancia, Intensitat
                    resistencia, reactancia, intensitat = ['0,001', '0,001', '0,001']
                    if tram.get('cable', False):
                        cable_obj = O.GiscedataAtCables
                        cable_id = tram['cable'][0]
                        cable_data = cable_obj.read(cable_id, ['resistencia', 'reactancia', 'intensitat_admisible'])
                        if cable_data.get('resistencia', False):    
                            resistencia = format_f(
                                cable_data['resistencia'] * (float(tram['longitud_cad']) *
                                                             coeficient / 1000.0) or 0.0,
                                decimals=3)
                        if cable_data.get('reactancia', False):    
                            reactancia = format_f(
                                cable_data['reactancia'] * (float(tram['longitud_cad']) *
                                                            coeficient / 1000.0) or 0.0,
                                decimals=3)
                        if cable_data.get('resistencia', False):    
                            intensitat = format_f(
                                cable_data['intensitat_admisible'] or 0.0, decimals=3)

                    cable_data_val = [resistencia, reactancia, intensitat]
                    for index, val in enumerate(cable_data_val):
                        if val == '0,000':
                            cable_data_val[index] = '0,001'
                    resistencia, reactancia, intensitat = cable_data_val

                    # Modelo
                    if tram.get('model', False):
                        modelo = tram['model']
                    else:
                        modelo = ''

                    # Punt frontera
                    punt_frontera = '0'
                    if tram.get('punt_frontera', False):
                        punt_frontera = '1'

                    # Operación
                    operacion = '1'
                    if not tram.get('operacion', False):
                        operacion = '0'

                    # Causa baja
                    if tram.get('obra_id', False):
                        causa_baja = '0'
                        obra_id = tram['obra_id']
                        obra_at_obj = O.GiscedataProjecteObraTiAt
                        causa_baja_data = obra_at_obj.read(obra_id, ['causa_baja'])
                        if causa_baja_data.get('causa_baja', False):
                            causa_baja = causa_baja_data['causa_baja']
                    else:
                        causa_baja = '0'

                    # Fecha APS
                    fecha_aps = ''
                    if tram['data_pm']:
                        data_pm_linia = datetime.strptime(str(tram['data_pm']),
                                                          '%Y-%m-%d')
                        fecha_aps = data_pm_linia.strftime('%d/%m/%Y')


                    # OBRES

                    fields_to_read_obra = [
                        'name', 'cini', 'tipo_inversion', 'ccuu', 'codigo_ccaa', 'nivel_tension_explotacion',
                        'financiado', 'fecha_aps', 'fecha_baja', 'causa_baja', 'im_ingenieria', 'im_materiales',
                        'im_obracivil', 'im_trabajos', 'subvenciones_europeas', 'subvenciones_nacionales',
                        'subvenciones_prtr', 'avifauna', 'valor_auditado', 'valor_contabilidad', 'cuenta_contable',
                        'porcentaje_modificacion', 'motivacion', 'obra_id', 'identificador_baja'
                    ]

                    # OBRES
                    tram_obra = ''
                    obra_ti_at_obj = O.GiscedataProjecteObraTiAt
                    obra_ti_ids = obra_ti_at_obj.search([('element_ti_id', '=', tram['id'])])
                    if obra_ti_ids:
                        for obra_ti_id in obra_ti_ids:
                            obra_id_data = obra_ti_at_obj.read(obra_ti_id, ['obra_id'])
                            obra_id = obra_id_data['obra_id']
                            # Filtre d'obres finalitzades
                            data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                            if data_finalitzacio_data:
                                if data_finalitzacio_data.get('data_finalitzacio', False):
                                    data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                                    inici_any = '{}-01-01'.format(self.year)
                                    fi_any = '{}-12-31'.format(self.year)
                                    if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                        tram_obra = O.GiscedataProjecteObraTiAt.read(obra_ti_id, fields_to_read_obra)
                            if tram_obra:
                                break

                    tipo_inversion = ''
                    financiado = ''

                    # CAMPS OBRA
                    if tram_obra != '':
                        obra_year = data_finalitzacio.split('-')[0]
                        data_pm_year = (
                                fecha_aps and fecha_aps.split('/')[2] or ''
                        )
                        if tram_obra['tipo_inversion'] != '0' and obra_year != data_pm_year:
                            data_ip = convert_spanish_date(data_finalitzacio)
                        else:
                            data_ip = ''
                        if tram_obra.get('identificador_baja', False):
                            tram_id = tram_obra['identificador_baja'][0]
                            tram_data = O.GiscedataAtTram.read(tram_id, ['name', 'id_regulatori'])
                            if tram_data.get('id_regulatori', False):
                                identificador_baja = tram_data['id_regulatori']
                            else:
                                identificador_baja = '{}{}'.format(self.prefix_AT, tram_data['name'])
                        else:
                            identificador_baja = ''
                        tipo_inversion = tram_obra['tipo_inversion'] or ''
                        im_ingenieria = format_f_6181(tram_obra['im_ingenieria'] or 0.0, float_type='euro')
                        im_materiales = format_f_6181(tram_obra['im_materiales'] or 0.0, float_type='euro')
                        im_obracivil = format_f_6181(tram_obra['im_obracivil'] or 0.0, float_type='euro')
                        im_construccion = str(format_f(
                            float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", "."))
                            , 2)).replace(".", ",")
                        im_trabajos = format_f_6181(tram_obra['im_trabajos'] or 0.0, float_type='euro')
                        subvenciones_europeas = format_f_6181(tram_obra['subvenciones_europeas'] or 0.0, float_type='euro')
                        subvenciones_nacionales = format_f_6181(tram_obra['subvenciones_nacionales'] or 0.0,
                                                                float_type='euro')
                        subvenciones_prtr = format_f_6181(tram_obra['subvenciones_prtr'] or 0.0, float_type='euro')
                        valor_auditado = format_f_6181(tram_obra['valor_auditado'] or 0.0, float_type='euro')
                        motivacion = get_codi_actuacio(O, tram_obra['motivacion'] and tram_obra['motivacion'][0]) if not \
                            tram_obra['fecha_baja'] else ''
                        cuenta_contable = tram_obra['cuenta_contable'] or ''
                        avifauna = int(tram_obra['avifauna'] == True)
                        financiado = format_f(tram_obra['financiado'], decimals=2) or ''
                    else:
                        data_ip = ''
                        identificador_baja = ''
                        im_ingenieria = ''
                        im_construccion = ''
                        im_trabajos = ''
                        subvenciones_europeas = ''
                        subvenciones_nacionales = ''
                        subvenciones_prtr = ''
                        valor_auditado = ''
                        motivacion = ''
                        cuenta_contable = ''
                        avifauna = ''

                    # CAUSA_BAJA
                    causa_baja = '0'
                    if tram_obra:
                        if tram_obra.get('causa_baja', False):
                            causa_baja = tram_obra['causa_baja']

                    # Node inicial / Node final
                    o_nivell_tensio = ''
                    if tram.get('tensio_max_disseny_id', False):
                        nivell_tensio_id = tram['tensio_max_disseny_id'][0]
                        o_nivell_tensio = self.tensions.get(nivell_tensio_id, '')
                    else:
                        if tram.get('linia', False):
                            linia_id, linia_name = tram['linia']
                            tensio_data = O.GiscedataAtLinia.read(linia_id, ['tensio_id'])
                            if tensio_data.get('tensio_id', False):
                                o_nivell_tensio = self.tensions.get(tensio_data['tensio_id'][0], '')

                    # identificador_tramo
                    if tram.get('id_regulatori', False):
                        o_tram = tram['id_regulatori']
                    else:
                        o_tram = '{}{}'.format(self.prefix_AT, tram['name'])

                    if 'edge_id' in O.GiscedataAtTram.fields_get().keys():
                        tram_edge = O.GiscedataAtTram.read(
                            tram['id'], ['edge_id']
                        )['edge_id']
                        if not tram_edge:
                            res = O.GiscegisEdge.search(
                                [
                                    ('id_linktemplate', '=', tram['name']),
                                    ('layer', 'not ilike', self.layer),
                                    ('layer', 'not ilike', 'EMBARRA%BT%')
                                ]
                            )
                            if not res or len(res) > 1:
                                edge = {
                                    'start_node': (0, '%s_0' % tram['name']),
                                    'end_node': (0, '%s_1' % tram['name'])
                                }
                            else:
                                edge = O.GiscegisEdge.read(
                                    res[0], ['start_node', 'end_node']
                                )
                        else:
                            edge = O.GiscegisEdge.read(
                                tram_edge[0], ['start_node', 'end_node']
                            )
                    else:
                        res = O.GiscegisEdge.search(
                            [
                                ('id_linktemplate', '=', tram['name']),
                                ('layer', 'not ilike', self.layer),
                                ('layer', 'not ilike', 'EMBARRA%BT%')
                            ]
                        )
                        if not res or len(res) > 1:
                            edge = {
                                'start_node': (0, '%s_0' % tram['name']),
                                'end_node': (0, '%s_1' % tram['name'])
                            }
                        else:
                            edge = O.GiscegisEdge.read(
                                res[0], ['start_node', 'end_node']
                            )

                    node_inicial = tallar_text(edge['start_node'][1], 20)
                    node_inicial = node_inicial.replace('*', '')
                    if node_inicial in self.nodes_red:
                        node_inicial = "{}-{}".format(
                            node_inicial, o_nivell_tensio
                        )
                    node_final = tallar_text(edge['end_node'][1], 20)
                    node_final = node_final.replace('*', '')
                    if node_final in self.nodes_red:
                        node_final = "{}-{}".format(
                            node_final, o_nivell_tensio
                        )

                    if tram.get('data_baixa'):
                        if tram.get('data_baixa') > data_pm_limit:
                            fecha_baja = ''
                        else:
                            tmp_date = datetime.strptime(
                                tram.get('data_baixa'), '%Y-%m-%d')
                            fecha_baja = tmp_date.strftime('%d/%m/%Y')
                    else:
                        fecha_baja = ''

                    # Fecha APS / Estado
                    if modelo == 'M':
                        estado = ''
                        fecha_aps = ''
                        operacion = 0
                    else:
                        # Estado
                        # Busquem a l'historic
                        hist_obj = O.model('circular.82021.historics.b1')
                        hist_ids = hist_obj.search([
                            ('identificador_tramo', '=', o_tram),
                            ('year', '=', self.year - 1)
                        ])
                        if hist_ids:
                            hist = hist_obj.read(hist_ids[0], [
                                'cini', 'codigo_ccuu', 'longitud',
                                'tension_explotacion', 'fecha_aps'
                            ])
                            entregada = F1Res4666(
                                cini=hist['cini'],
                                longitud=hist['longitud'],
                                codigo_ccuu=hist['codigo_ccuu'],
                                nivel_tension=hist['tension_explotacion'],
                                fecha_aps=hist['fecha_aps']
                            )
                            actual = F1Res4666(
                                o_tram,
                                tram['cini'],
                                node_inicial or edge['start_node'][1],
                                node_final or edge['end_node'][1],
                                codi_ccuu,
                                comunitat,
                                comunitat,
                                '',
                                fecha_aps,
                                fecha_baja,
                                tram.get('circuits', 1) or 1,
                                1,
                                tension_explotacion,
                                format_f(longitud, 3),
                                intensitat,
                                '',
                                '',
                                0
                            )
                            estado = calculate_estado(
                                fecha_baja, actual, entregada, tram_obra)
                            if estado == '1' and not tram_obra:
                                self.output_m.put("{} {}".format(
                                    tram["name"],
                                    adapt_diff(actual.diff(entregada))))
                        else:
                            estado = default_estado(
                                modelo, fecha_aps, int(self.year))

                    if fecha_baja:
                        motivacion = ''
                        tipo_inversion = ''

                    # L'any 2022 no es declaren subvencions PRTR
                    subvenciones_prtr = ''

                    if causa_baja == '0':
                        fecha_baja = ''

                    if modelo == 'E' and estado == '2':
                        tipo_inversion = '0'

                    # Buidem FECHA_IP si hi ha FECHA_BAJA
                    if fecha_baja:
                        data_ip = ''

                    output = [
                        o_tram,  # IDENTIFICADOR
                        tram.get('cini', '') or '',         # CINI
                        codi_ccuu or '',                    # CODIGO_CCUU
                        node_inicial or edge['start_node'][1],    # ORIGEN
                        node_final or edge['end_node'][1],       # DESTINO
                        comunitat,                          # CODIGO_CCAA_1
                        comunitat,                          # CODIGO_CCAA_2
                        propietari,                         # PROPIEDAD
                        tension_explotacion,                # TENSION_EXPLOTACIÓN
                        tension_construccion,               # TENSION_CONSTRUCCIÓN
                        format_f(longitud, 3),              # LONGITUD
                        resistencia,                        # RESISTENCIA
                        reactancia,                         # REACTANCIA
                        intensitat,                         # INTENSIDAD
                        estado,                             # ESTADO
                        punt_frontera,                      # PUNTO_FRONTERA
                        modelo,                             # MODELO
                        operacion,                          # OPERACIÓN
                        fecha_aps,                          # FECHA APS
                        causa_baja,                         # CAUSA BAJA
                        fecha_baja or '',                   # FECHA BAJA
                        data_ip,                            # FECHA IP
                        tipo_inversion,                     # TIPO INVERSION
                        motivacion,                         # MOTIVACION
                        im_ingenieria,                      # IM_TRAMITES
                        im_construccion,                    # IM_CONSTRUCCION
                        im_trabajos,                        # IM_TRABAJOS
                        valor_auditado,  # VALOR AUDITADO
                        financiado,                         # FINANCIADO
                        subvenciones_europeas,              # SUBVENCIONES EUROPEAS
                        subvenciones_nacionales,            # SUBVENCIONES NACIONALES
                        subvenciones_prtr,                  # SUBVENCIONES PRTR
                        cuenta_contable,                    # CUENTA CONTABLE
                        avifauna,                           # AVIFAUNA
                        identificador_baja,                 # ID_BAJA
                    ]

                    self.output_q.put(output)

                if model == 'bt':
                    fields_to_read = ['name', 'cini', 'coeficient', 'municipi', 'voltatge', 'tensio_const',
                                      'coeficient', 'longitud_cad', 'punt_frontera', 'model', 'operacion',
                                      'propietari', 'edge_id', 'cable', 'tipus_instalacio_cnmc_id',
                                      'data_pm', 'data_baixa', 'id_regulatori',
                                      'perc_financament',
                                      ]

                    linia = O.GiscedataBtElement.read(item, fields_to_read)

                    # Identificador_tramo
                    if linia.get('id_regulatori', False):
                        identificador_tramo = linia['id_regulatori']
                    else:
                        identificador_tramo = '{}{}'.format(self.prefix_BT, linia['name'])

                    # CINI
                    cini = ''
                    if linia.get('cini', False):
                        cini = linia['cini']

                    # CODIGO CCUU
                    if linia.get('tipus_instalacio_cnmc_id', False):
                        id_ti = linia['tipus_instalacio_cnmc_id'][0]
                        codigo_ccuu = O.GiscedataTipusInstallacio.read(
                            id_ti,
                            ['name'])['name']
                    else:
                        codigo_ccuu = ''

                    # NUDO INICIAL / NUDO FINAL
                    if 'edge_id' in O.GiscedataBtElement.fields_get().keys():
                        bt_edge = O.GiscedataBtElement.read(
                            linia['id'], ['edge_id']
                        )['edge_id']
                        if not bt_edge:
                            res = O.GiscegisEdge.search(
                                [('id_linktemplate', '=', linia['name']),
                                 '|',
                                 ('layer', 'ilike', self.layer),
                                 ('layer', 'ilike', 'EMBARRA%BT%')
                                 ])
                            if not res or len(res) > 1:
                                edge = {'start_node': (0, '%s_0' % linia['name']),
                                        'end_node': (0, '%s_1' % linia['name'])}
                            else:
                                edge = O.GiscegisEdge.read(
                                    res[0], ['start_node', 'end_node']
                                )
                        else:
                            edge = O.GiscegisEdge.read(
                                bt_edge[0], ['start_node', 'end_node']
                            )
                    else:
                        res = O.GiscegisEdge.search(
                            [('id_linktemplate', '=', linia['name']),
                             '|',
                             ('layer', 'ilike', self.layer),
                             ('layer', 'ilike', 'EMBARRA%BT%')
                             ])
                        if not res or len(res) > 1:
                            edge = {'start_node': (0, '%s_0' % linia['name']),
                                    'end_node': (0, '%s_1' % linia['name'])}
                        else:
                            edge = O.GiscegisEdge.read(res[0], ['start_nOde',
                                                                'end_node'])
                    node_inicial = tallar_text(edge['start_node'][1], 20)
                    node_inicial = node_inicial.replace('*', '')
                    node_final = tallar_text(edge['end_node'][1], 20)
                    node_final = node_final.replace('*', '')

                    # CCAA 1 / CCAA 2
                    ccaa_1 = ccaa_2 = ''
                    if linia['municipi']:
                        ccaa_obj = O.ResComunitat_autonoma
                        id_comunitat = ccaa_obj.get_ccaa_from_municipi(
                            linia['municipi'][0])
                        id_comunitat = id_comunitat[0]
                        comunidad = ccaa_obj.read(id_comunitat, ['codi'])
                        if comunidad:
                            ccaa_1 = ccaa_2 = comunidad['codi']

                    # PROPIEDAD
                    propiedad = '0'
                    if linia.get('propietari', False):
                        if linia['propietari']:
                            propiedad = '1'
                        else:
                            propiedad = '0'

                    # TENSION EXPLOTACION
                    tension_explotacion = ''
                    if linia.get('voltatge', False):
                        tension_explotacion = format_f(float(linia['voltatge'])/1000, 3)

                    # TENSION CONSTRUCCION
                    tension_construccion = ''
                    if linia.get('tensio_const', False):
                        tension_construccion = self.tensions.get(linia['tensio_const'][0], '')
                        if str(tension_construccion) == str(tension_explotacion):
                            tension_construccion = ''

                    # LONGITUD
                    coeficient = linia.get('coeficient', 1.0)
                    if 'longitud_cad' in linia:
                        if self.dividir:
                            long_tmp = linia['longitud_cad']/linia.get(
                                'circuits', 1
                            ) or 1
                            longitud = round(
                                long_tmp * coeficient/1000.0, 3
                            ) or 0.001
                        else:
                            longitud = round(
                                linia['longitud_cad'] * coeficient/1000.0, 3
                            ) or 0.001
                    else:
                        longitud = 0

                    # RESISTENCIA, REACTANCIA, INTENSITAT
                    resistencia, reactancia, intensitat = ['0,001', '0,001', '0,001']
                    if linia.get('cable', False):
                        cable_obj = O.GiscedataBtCables
                        cable_id = linia['cable'][0]
                        cable_data = cable_obj.read(cable_id, ['resistencia', 'reactancia', 'intensitat_admisible'])
                        longitud = format_f(
                            round(float(linia['longitud_cad']) *
                                  coeficient / 1000.0, 3) or 0.001, decimals=3)
                        if cable_data.get('resistencia', False):    
                            resistencia = format_f(
                                cable_data['resistencia'] * (float(linia['longitud_cad']) *
                                                             coeficient / 1000.0) or 0.0,
                                decimals=3)
                        if cable_data.get('reactancia', False):    
                            reactancia = format_f(
                                cable_data['reactancia'] * (float(linia['longitud_cad']) *
                                                            coeficient / 1000.0) or 0.0,
                                decimals=3)
                        if cable_data.get('resistencia', False):    
                            intensitat = format_f(
                                cable_data['intensitat_admisible'] or 0.0, decimals=3)

                    cable_data_val = [resistencia, reactancia, intensitat]
                    for index, val in enumerate(cable_data_val):
                        if val == '0,000':
                            cable_data_val[index] = '0,001'
                    resistencia, reactancia, intensitat = cable_data_val

                    # PUNTO FRONTERA
                    punto_frontera = '0'
                    if linia.get('punt_frontera', False):
                        punto_frontera = '1'

                    # OPERACION
                    operacion = '1'
                    if not linia.get('operacion', False):
                        operacion = '0'

                    # FECHA BAJA
                    if linia.get('data_baixa'):
                        if linia.get('data_baixa') > data_pm_limit:
                            fecha_baja = ''
                        else:
                            tmp_date = datetime.strptime(
                                linia.get('data_baixa'), '%Y-%m-%d')
                            fecha_baja = tmp_date.strftime('%d/%m/%Y')
                    else:
                        fecha_baja = ''

                    # Fecha APS
                    fecha_aps = ''
                    if linia['data_pm']:
                        data_pm_linia = datetime.strptime(str(linia['data_pm']),
                                                          '%Y-%m-%d')
                        fecha_aps = data_pm_linia.strftime('%d/%m/%Y')

                    # OBRES
                    fields_to_read_obra = [
                        'name', 'cini', 'tipo_inversion', 'ccuu', 'codigo_ccaa', 'nivel_tension_explotacion',
                        'financiado', 'fecha_aps', 'fecha_baja', 'causa_baja', 'im_ingenieria', 'im_materiales',
                        'im_obracivil', 'im_trabajos', 'subvenciones_europeas', 'subvenciones_nacionales',
                        'subvenciones_prtr', 'avifauna', 'valor_auditado', 'valor_contabilidad', 'cuenta_contable',
                        'porcentaje_modificacion', 'motivacion', 'obra_id', 'identificador_baja'
                    ]

                    linia_obra = ''
                    obra_ti_bt_obj = O.GiscedataProjecteObraTiBt
                    obra_ti_ids = obra_ti_bt_obj.search([('element_ti_id', '=', linia['id'])])
                    if obra_ti_ids:
                        for obra_ti_id in obra_ti_ids:
                            obra_id_data = obra_ti_bt_obj.read(obra_ti_id, ['obra_id'])
                            obra_id = obra_id_data['obra_id']
                            # Filtre d'obres finalitzades
                            data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                            if data_finalitzacio_data:
                                if data_finalitzacio_data.get('data_finalitzacio', False):
                                    data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                                    inici_any = '{}-01-01'.format(self.year)
                                    fi_any = '{}-12-31'.format(self.year)
                                    if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                        linia_obra = O.GiscedataProjecteObraTiBt.read(obra_ti_id, fields_to_read_obra)
                            if linia_obra:
                                break

                    tipo_inversion = ''
                    financiado = ''

                    # CAMPS OBRA
                    if linia_obra != '':
                        obra_year = data_finalitzacio.split('-')[0]
                        data_pm_year = (
                                fecha_aps and fecha_aps.split('/')[2] or ''
                        )
                        if linia_obra['tipo_inversion'] != '0' and obra_year != data_pm_year:
                            data_ip = convert_spanish_date(data_finalitzacio)
                        else:
                            data_ip = ''
                        if linia_obra.get('identificador_baja', False):
                            elem_id = linia_obra['identificador_baja'][0]
                            elem_data = O.GiscedataBtElement.read(elem_id, ['name', 'id_regulatori'])
                            if elem_data.get('id_regulatori', False):
                                identificador_baja = elem_data['id_regulatori']
                            else:
                                identificador_baja = '{}{}'.format(self.prefix_BT, elem_data['name'])

                        else:
                            identificador_baja = ''
                        tipo_inversion = linia_obra['tipo_inversion'] or ''
                        im_ingenieria = format_f_6181(linia_obra['im_ingenieria'] or 0.0, float_type='euro')
                        im_materiales = format_f_6181(linia_obra['im_materiales'] or 0.0, float_type='euro')
                        im_obracivil = format_f_6181(linia_obra['im_obracivil'] or 0.0, float_type='euro')
                        im_construccion = str(format_f(
                            float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", "."))
                            , 2)).replace(".", ",")
                        im_trabajos = format_f_6181(linia_obra['im_trabajos'] or 0.0, float_type='euro')
                        subvenciones_europeas = format_f_6181(linia_obra['subvenciones_europeas'] or 0.0,
                                                              float_type='euro')
                        subvenciones_nacionales = format_f_6181(linia_obra['subvenciones_nacionales'] or 0.0,
                                                                float_type='euro')
                        subvenciones_prtr = format_f_6181(linia_obra['subvenciones_prtr'] or 0.0, float_type='euro')
                        valor_auditado = format_f_6181(linia_obra['valor_auditado'] or 0.0, float_type='euro')
                        motivacion = get_codi_actuacio(O, linia_obra['motivacion'] and linia_obra['motivacion'][0]) if not \
                            linia_obra['fecha_baja'] else ''
                        cuenta_contable = linia_obra['cuenta_contable'] or ''
                        avifauna = int(linia_obra['avifauna'] == True)
                        financiado = format_f(linia_obra['financiado'], decimals=2) or ''

                    else:
                        data_ip = ''
                        identificador_baja = ''
                        im_ingenieria = ''
                        im_construccion = ''
                        im_trabajos = ''
                        subvenciones_europeas = ''
                        subvenciones_nacionales = ''
                        subvenciones_prtr = ''
                        valor_auditado = ''
                        motivacion = ''
                        cuenta_contable = ''
                        avifauna = ''

                    # CAUSA_BAJA
                    causa_baja = '0'
                    if linia_obra:
                        if linia_obra.get('causa_baja', False):
                            causa_baja = linia_obra['causa_baja']

                    # MODELO
                    modelo = ''
                    if linia.get('model', False):
                        modelo = linia['model']

                    # Fecha APS / Estado
                    if modelo == 'M':
                        estado = ''
                        fecha_aps = ''
                        operacion = 0
                    else:
                        # Estado
                        hist_obj = O.model('circular.82021.historics.b1')
                        hist_ids = hist_obj.search([
                            ('identificador_tramo', '=', identificador_tramo),
                            ('year', '=', self.year - 1)
                        ])
                        if hist_ids:
                            hist = hist_obj.read(hist_ids[0], [
                                'cini', 'codigo_ccuu', 'longitud',
                                'tension_explotacion', 'fecha_aps'
                            ])
                            entregada = F2Res4666(
                                cini=hist['cini'],
                                longitud=hist['longitud'],
                                codigo_ccuu=hist['codigo_ccuu'],
                                nivel_tension=hist['tension_explotacion'],
                                fecha_aps=hist['fecha_aps']
                            )
                            actual = F2Res4666(
                                identificador_tramo,
                                linia['cini'],
                                node_inicial,
                                node_final,
                                codigo_ccuu,
                                ccaa_1,
                                ccaa_2,
                                '',
                                fecha_aps,
                                fecha_baja,
                                1,
                                1,
                                tension_explotacion,
                                format_f(longitud, 3),
                                intensitat,
                                '',
                                '',
                                0
                            )
                            estado = calculate_estado(
                                fecha_baja, actual, entregada, linia_obra)
                            if estado == '1' and not linia_obra:
                                self.output_m.put("{} {}".format(
                                        linia["name"],
                                        adapt_diff(actual.diff(entregada))))
                        else:
                            estado = default_estado(
                                modelo, fecha_aps, int(self.year))

                    if fecha_baja:
                        motivacion = ''
                        tipo_inversion = ''

                    # L'any 2022 no es declaren subvencions PRTR
                    subvenciones_prtr = ''

                    if causa_baja == '0':
                        fecha_baja = ''

                    if modelo == 'E' and estado == '2':
                        tipo_inversion = '0'

                    # Buidem FECHA_IP si hi ha FECHA_BAJA
                    if fecha_baja:
                        data_ip = ''

                    output = [
                        identificador_tramo,  # IDENTIFICADOR TRAMO
                        linia.get('cini', '') or '',  # CINI
                        codigo_ccuu or '',  # CODIGO_CCUU
                        node_inicial,  # ORIGEN
                        node_final,  # DESTINO
                        ccaa_1,  # CODIGO_CCAA_1
                        ccaa_2,  # CODIGO_CCAA_2
                        propiedad,  # PROPIEDAD
                        tension_explotacion,  # TENSION_EXPLOTACIÓN
                        tension_construccion,  # TENSION_CONSTRUCCIÓN
                        format_f(longitud, 3),  # LONGITUD
                        resistencia,  # RESISTENCIA
                        reactancia,  # REACTANCIA
                        intensitat,  # INTENSIDAD
                        estado,  # ESTADO
                        punto_frontera,  # PUNTO_FRONTERA
                        modelo,  # MODELO
                        operacion,  # OPERACIÓN
                        fecha_aps,  # FECHA APS
                        causa_baja,  # CAUSA BAJA
                        fecha_baja or '',  # FECHA BAJA
                        data_ip,  # FECHA IP
                        tipo_inversion,  # TIPO INVERSION
                        motivacion,  # MOTIVACION
                        im_ingenieria,  # IM_TRAMITES
                        im_construccion,  # IM_CONSTRUCCION
                        im_trabajos,  # IM_TRABAJOS
                        valor_auditado,  # VALOR AUDITADO
                        format_f(financiado, decimals=2),  # FINANCIADO
                        subvenciones_europeas,  # SUBVENCIONES EUROPEAS
                        subvenciones_nacionales,  # SUBVENCIONES NACIONALES
                        subvenciones_prtr,  # SUBVENCIONES PRTR
                        cuenta_contable,  # CUENTA CONTABLE
                        avifauna,  # AVIFAUNA
                        identificador_baja,  # ID_BAJA
                    ]

                    self.output_q.put(output)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
