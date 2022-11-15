#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
from libcnmc.utils import (format_f, tallar_text, format_f_6181, get_codi_actuacio, convert_spanish_date,
                           get_forced_elements)

class FB1(MultiprocessBased):
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
        self.base_object = 'Línies AT'
        self.report_name = 'CNMC INVENTARI AT'
        self.layer = 'LBT\_%'
        self.embarrats = kwargs.pop('embarrats', False)
        self.linia_tram_include = {}
        self.forced_ids = {}
        self.prefix_AT = kwargs.pop('prefix_at', 'A') or 'A'
        self.prefix_BT = kwargs.pop('prefix_bt', 'B') or 'B'
        self.dividir = kwargs.pop('div', False)

        id_res_like = self.connection.ResConfig.search(
            [('name', '=', 'giscegis_btlike_layer')])
        if id_res_like:
            self.layer = self.connection.ResConfig.read(
                id_res_like, ['value'])[0]['value']

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer

        :return: List of ids
        :rtype: list(int)
        """

        # AT
        search_params = [
            ('active', '=', True),
            ('criteri_regulatori', '!=', 'excloure')
        ]
        obj_lat = self.connection.GiscedataAtLinia
        ids = obj_lat.search(search_params)
        at_ids = list(set(ids))

        for elem in range(0, len(at_ids)):
            at_ids[elem] = 'at.{}'.format(at_ids[elem])

        # BT
        search_params = [
            ('active', '=', True),
            ('criteri_regulatori', '!=', 'excloure')
        ]
        obj_lat = self.connection.GiscedataBtTram
        ids = obj_lat.search(search_params)
        bt_ids = list(set(ids))

        for elem in range(0, len(bt_ids)):
            bt_ids[elem] = 'bt.{}'.format(bt_ids[elem])

        # bt_ids = bt_ids[0:1]
        at_bt_ids = at_ids + bt_ids
        print('####################')
        print('####################')
        print('AT: {}'.format(len(at_ids)))
        print('BT: {}'.format(len(bt_ids)))
        print('####################')
        print('####################')
        return at_bt_ids

    def consumer(self):
        """
        Method that generates the csv file
        :return: List of arrays
        """
        O = self.connection

        fields_to_read = [
            'baixa', 'data_pm', 'data_industria', 'coeficient', 'cini',
            'propietari', 'tensio_max_disseny_id', 'name', 'origen', 'final',
            'perc_financament', 'longitud_cad', 'cable', 'linia', 'model',
            'tipus_instalacio_cnmc_id', 'data_baixa',
            'baixa', 'data_baixa', 'longitud_cad', 'model', 'punt_frontera', 'data_pm'
        ]
        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)

        static_search_params = [
            ('propietari', '=', True),
            '|', ('data_pm', '=', False), ('data_pm', '<', data_pm_limit),
            '|',
            '&', ('data_baixa', '>', data_baixa),
                 ('baixa', '=', True),
            '|',
                 ('data_baixa', '=', False),
                 ('baixa', '=', False)
            ]

        # print 'static_search_params:{}'.format(static_search_params)
        # Revisem que si està de baixa ha de tenir la data informada.
        static_search_params += [
            '|',
            '&', ('active', '=', False), ('data_baixa', '!=', False),
            ('active', '=', True)
        ]

        def get_inst_name(element_id):
            vals = self.connection.GiscedataCts.read(
                element_id[0], ['name'])
            return vals['name']

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                model = item.split('.')[0]
                item = int(item.split('.')[1])

                if model == 'at':
                    linia = O.GiscedataAtLinia.read(
                        item,
                        ['trams', 'tensio', 'municipi', 'propietari', 'provincia']
                    )

                    propietari = linia['propietari'] and '1' or '0'
                    search_params = [('linia', '=', linia['id'])]
                    search_params += static_search_params
                    ids = O.GiscedataAtTram.search(
                        search_params, 0, 0, False, {'active_test': False})

                    if item in self.linia_tram_include:
                        ids = list(set(ids + self.linia_tram_include[item]))

                    ids = list(set(ids) - set(self.forced_ids["exclude"]))

                    id_desconegut = O.GiscedataAtCables.search(
                        [('name', '=', 'DESCONEGUT')])

                    if not id_desconegut:
                        id_desconegut = O.GiscedataAtCables.search(
                            [('name', '=', 'DESCONOCIDO')])[0]
                    for tram in O.GiscedataAtTram.read(ids, fields_to_read):
                        if tram["baixa"] and tram["data_baixa"] is False:
                            continue
                        # Comprovar el tipus del cable
                        if 'cable' in tram:
                            cable = O.GiscedataAtCables.read(
                                tram['cable'][0], ['tipus'])
                            tipus = O.GiscedataAtTipuscable.read(
                                cable['tipus'][0], ['codi']
                            )
                            if self.embarrats and tram["longitud_cad"] >= 100 and tipus["codi"] == "E":
                                continue
                            if not self.embarrats and cable['tipus']:
                                # Si el tram tram es embarrat no l'afegim
                                if tipus['codi'] == 'E':
                                    continue
                        else:
                            cable = O.GiscedataAtCables.read(
                                id_desconegut, ['tipus'])

                        # Comprovar el tipus del cable
                        if 'cable' in tram:
                            cable = O.GiscedataAtCables.read(
                                tram['cable'][0], ['tipus'])
                            tipus = O.GiscedataAtTipuscable.read(
                                cable['tipus'][0], ['codi']
                            )
                            if self.embarrats and tram["longitud_cad"] >= 100 and tipus["codi"] == "E":
                                continue
                            if not self.embarrats and cable['tipus']:
                                # Si el tram tram es embarrat no l'afegim
                                if tipus['codi'] == 'E':
                                    continue

                        # Calculem any posada en marxa
                        data_pm = ''
                        if 'data_pm' in tram and tram['data_pm'] and tram['data_pm'] < data_pm_limit:
                            data_pm = datetime.strptime(str(tram['data_pm']),
                                                        '%Y-%m-%d')
                            data_pm = data_pm.strftime('%d/%m/%Y')

                        # Calculem la data de baixa
                        data_baixa = ''
                        if tram['data_baixa'] and tram['baixa']:
                            data_baixa = datetime.strptime(str(tram['data_baixa']),
                                                           '%Y-%m-%d')
                            data_baixa = data_baixa.strftime('%d/%m/%Y')

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
                        if tram.get('linia', False):
                            linia_id = O.GiscedataAtLinia.search([('name', '=', tram['linia'][1])])
                            if linia_id:
                                municipi_data = O.GiscedataAtLinia.read(linia_id, ['municipi'])[0]
                                if municipi_data.get('municipi', False):
                                    municipi_id = municipi_data['municipi'][0]
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
                            propietari = ''

                        # Tensión_explotación
                        tension_explotacion = ''
                        if tram.get('linia', False):
                            linia_name = tram['linia'][1]
                            linia_srch = O.GiscedataAtLinia.search([('name', '=', linia_name)])
                            if linia_srch:
                                tensio_data = O.GiscedataAtLinia.read(linia_srch, ['tensio'])[0]
                                if tensio_data.get('tensio', False):
                                    tension_explotacion = tensio_data['tensio']

                        # Tensión_construcción
                        tension_construccion = ''
                        if tram.get('tensio_max_disseny_id', False):
                            tension_construccion = tram['tensio_max_disseny_id'][1]
                            if str(tension_construccion) == str(tension_explotacion):
                                tension_construccion = ''
                            else:
                                tension_construccion = ''

                        # Resistencia, Reactancia, Intensitat
                        if tram.get('cable', False):
                            cable_obj = O.GiscedataAtCables
                            cable_id = tram['cable'][0]
                            cable_data = cable_obj.read(cable_id, ['resistencia', 'reactancia', 'intensitat_admisible'])
                            if tram.get('longitud_cad', False):
                                longitud_en_km = tram['longitud_cad'] / 1000
                                if cable_data.get('resistencia', False):
                                    resistencia_per_km = cable_data['resistencia']
                                    resistencia = resistencia_per_km * longitud_en_km
                                else:
                                    resistencia = ''
                                if cable_data.get('reactancia', False):
                                    reactancia_per_km = cable_data['reactancia']
                                    reactancia = reactancia_per_km * longitud_en_km
                                else:
                                    reactancia = ''
                            else:
                                resistencia, reactancia = ['', '']
                            if cable_data.get('intensitat_admisible',  False):
                                intensitat = cable_data['intensitat_admisible']
                            else:
                                intensitat = ''
                        else:
                            resistencia, reactancia, intensitat = ['', '', '']

                        # Modelo
                        if tram.get('model', False):
                            modelo = tram['model']
                        else:
                            modelo = ''

                        # Punt frontera
                        if tram.get('punt_frontera', False):
                            punt_frontera = tram['punt_frontera']
                        else:
                            punt_frontera = ''

                        # Operación
                        if tram.get('operacion', False):
                            operacion = tram['operacion']
                        else:
                            operacion = ''

                        # Fechas APS
                        if tram.get('data_pm', False):
                            data_pm = tram['data_pm']
                        else:
                            data_pm = ''

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

                        # OBRES

                        fields_to_read_obra = [
                            'name', 'cini', 'tipo_inversion', 'ccuu', 'codigo_ccaa', 'nivel_tension_explotacion',
                            'financiado', 'fecha_aps', 'fecha_baja', 'causa_baja', 'im_ingenieria', 'im_materiales',
                            'im_obracivil', 'im_trabajos', 'subvenciones_europeas', 'subvenciones_nacionales',
                            'subvenciones_prtr', 'avifauna', 'valor_auditado', 'valor_contabilidad', 'cuenta_contable',
                            'porcentaje_modificacion', 'motivacion', 'obra_id', 'identificador_baja'
                        ]

                        obra_id = O.GiscedataProjecteObraTiAt.search([('element_ti_id', '=', tram['id'])])

                        # Filtre d'obres finalitzades
                        tram_obra = ''
                        if obra_id:
                            data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                            if data_finalitzacio_data:
                                if data_finalitzacio_data.get('data_finalitzacio', False):
                                    data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                                    inici_any = '{}-01-01'.format(self.year)
                                    fi_any = '{}-12-31'.format(self.year)
                                    if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                        tram_obra = O.GiscedataProjecteObraTiAt.read(obra_id, fields_to_read_obra)[0]
                        else:
                            tram_obra = ''

                        # CAMPS OBRA
                        if tram_obra != '':
                            data_ip = convert_spanish_date(
                                tram_obra['fecha_aps'] if not tram_obra['fecha_baja'] and tram_obra['tipo_inversion'] != '1' else ''
                            )
                            identificador_baja = (
                                get_inst_name(tram_obra['identificador_baja']) if tram_obra['identificador_baja'] else ''
                            )
                            tipo_inversion = (tram_obra['tipo_inversion'] or '0') if not tram_obra['fecha_baja'] else '1'
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
                            cuenta_contable = tram_obra['cuenta_contable']
                            financiado = format_f(
                                100.0 - tram_obra.get('financiado', 0.0), 2
                            )
                            avifauna = int(tram_obra['avifauna'] == True)
                        else:
                            data_ip = ''
                            identificador_baja = ''
                            tipo_inversion = ''
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
                            financiado = ''

                        # Descripció
                        origen = tallar_text(tram['origen'], 50)
                        final = tallar_text(tram['final'], 50)
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
                        if not origen or not final:
                            res = O.GiscegisEdge.search(
                                [
                                    ('id_linktemplate', '=', tram['name']),
                                    ('layer', 'not ilike', self.layer),
                                    ('layer', 'not ilike', 'EMBARRA%BT%')
                                ])
                            if not res or len(res) > 1:
                                edge = {'start_node': (0, '{0}_0'.format(tram.get('name'))),
                                        'end_node': (0, '{0}_1'.format(tram.get('name')))}
                            else:
                                edge = O.GiscegisEdge.read(res[0], ['start_node',
                                                                    'end_node'])
                        if tram.get('data_baixa'):
                            if tram.get('data_baixa') > data_pm_limit:
                                fecha_baja = ''
                            else:
                                tmp_date = datetime.strptime(
                                    tram.get('data_baixa'), '%Y-%m-%d')
                                fecha_baja = tmp_date.strftime('%d/%m/%Y')
                        else:
                            fecha_baja = ''

                        output = [
                            '{}{}'.format(self.prefix_AT, tram['name']),  # IDENTIFICADOR
                            tram.get('cini', '') or '',         # CINI
                            codi_ccuu or '',                    # CODIGO_CCUU
                            origen or edge['start_node'][1],    # ORIGEN
                            final or edge['end_node'][1],       # DESTINO
                            comunitat,                          # CODIGO_CCAA_1
                            comunitat,                          # CODIGO_CCAA_2
                            propietari,                         # PROPIEDAD
                            tension_explotacion,                # TENSION_EXPLOTACIÓN
                            tension_construccion,               # TENSION_CONSTRUCCIÓN
                            format_f(longitud, 3),              # LONGITUD
                            resistencia,                        # RESISTENCIA
                            reactancia,                         # REACTANCIA
                            intensitat,                         # INTENSIDAD
                            punt_frontera,                      # PUNTO_FRONTERA
                            modelo,                             # MODELO
                            operacion,                          # OPERACIÓN
                            data_pm,                            # FECHA APS
                            causa_baja,                         # CAUSA BAJA
                            fecha_baja or '',                   # FECHA BAJA
                            data_ip,                            # FECHA IP
                            tipo_inversion,                     # TIPO INVERSION
                            im_ingenieria,                      # IM_TRAMITES
                            im_construccion,                    # IM_CONSTRUCCION
                            im_trabajos,                        # IM_TRABAJOS
                            subvenciones_europeas,              # SUBVENCIONES EUROPEAS
                            subvenciones_nacionales,            # SUBVENCIONES NACIONALES
                            subvenciones_prtr,                  # SUBVENCIONES PRTR
                            valor_auditado,                     # VALOR AUDITADO
                            financiado,                         # FINANCIADO
                            cuenta_contable,                    # CUENTA CONTABLE
                            motivacion,                         # MOTIVACION
                            avifauna,                           # AVIFAUNA
                            identificador_baja,                 # ID_BAJA
                        ]

                        self.output_q.put(output)

                if model == 'bt':
                    fields_to_read = ['name', 'cini', 'coeficient', 'municipi', 'voltatge', 'tensio_const',
                                      'coeficient', 'longitud_cad', 'punto_frontera', 'model', 'operacion',
                                      'propietari', 'edge_id', 'cable', 'tipus_instalacio_cnmc_id',
                                      'data_pm'
                                      ]

                    linia = O.GiscedataBtElement.read(item, fields_to_read)

                    # IDENTIFICADOR TRAMO
                    identificador_tramo = ''
                    if linia.get('name', False):
                        identificador_tramo = linia['name']

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
                    nudo_inicial = ''
                    nudo_final = ''
                    if linia.get('edge_id', False):
                        edge_id = linia['edge_id'][0]
                        edge_data = O.GiscegisEdge.read(edge_id, ['start_node', 'end_node'])
                        if edge_data.get('start_node', False):
                            nudo_inicial = edge_data['start_node'][1]
                        if edge_data.get('end_node', False):
                            nudo_final = edge_data['end_node'][1]

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
                    propiedad = ''
                    if linia.get('propietari', False):
                        if linia['propietari']:
                            propiedad = '1'
                        else:
                            propiedad = '0'

                    # TENSION EXPLOTACION
                    tension_explotacion = ''
                    if linia.get('voltatge', False):
                        tension_explotacion = linia['voltatge']

                    # TENSION CONSTRUCCION
                    tension_construccion = ''
                    if linia.get('tensio_const', False):
                        tension_construccion = linia['tensio_max_disseny_id'][1]
                        if str(tension_construccion) == str(tension_explotacion):
                            tension_construccion = ''
                        else:
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
                                tram['longitud_cad'] * coeficient/1000.0, 3
                            ) or 0.001
                    else:
                        longitud = 0

                    # RESISTENCIA, REACTANCIA, INTENSITAT
                    if tram.get('cable', False):
                        cable_obj = O.GiscedataAtCables
                        cable_id = tram['cable'][0]
                        cable_data = cable_obj.read(cable_id, ['resistencia', 'reactancia', 'intensitat_admisible'])
                        if tram.get('longitud_cad', False):
                            longitud_en_km = tram['longitud_cad'] / 1000
                            if cable_data.get('resistencia', False):
                                resistencia_per_km = cable_data['resistencia']
                                resistencia = resistencia_per_km * longitud_en_km
                            else:
                                resistencia = ''
                            if cable_data.get('reactancia', False):
                                reactancia_per_km = cable_data['reactancia']
                                reactancia = reactancia_per_km * longitud_en_km
                            else:
                                reactancia = ''
                        else:
                            resistencia, reactancia = ['', '']
                        if cable_data.get('intensitat_admisible', False):
                            intensitat = cable_data['intensitat_admisible']
                        else:
                            intensitat = ''
                    else:
                        resistencia, reactancia, intensitat = ['', '', '']

                    # ESTADO
                    estado = ''

                    # PUNTO FRONTERA
                    punto_frontera = ''
                    if linia.get('punto_frontera', False):
                        punto_frontera = linia['punto_frontera']

                    # MODELO
                    modelo = ''
                    if linia.get('model', False):
                        modelo = linia['model']

                    # OPERACION
                    operacion = ''
                    if linia.get('operacion', False):
                        if linia['operacion']:
                            operacion = '1'
                        else:    
                            operacion = '0'

                    # FECHA APS
                    fecha_aps = ''
                    if linia.get('data_pm', False):
                        fecha_aps = linia['data_pm']

                    # CAUSA BAJA
                    if linia.get('obra_id', False):
                        causa_baja = '0'
                        obra_id = linia['obra_id']
                        obra_at_obj = O.GiscedataProjecteObraTiAt
                        causa_baja_data = obra_at_obj.read(obra_id, ['causa_baja'])
                        if causa_baja_data.get('causa_baja', False):
                            causa_baja = causa_baja_data['causa_baja']
                    else:
                        causa_baja = '0'

                    # FECHA BAJA
                    if tram.get('data_baixa'):
                        if tram.get('data_baixa') > data_pm_limit:
                            fecha_baja = ''
                        else:
                            tmp_date = datetime.strptime(
                                tram.get('data_baixa'), '%Y-%m-%d')
                            fecha_baja = tmp_date.strftime('%d/%m/%Y')
                    else:
                        fecha_baja = ''

                    # OBRES
                    fields_to_read_obra = [
                        'name', 'cini', 'tipo_inversion', 'ccuu', 'codigo_ccaa', 'nivel_tension_explotacion',
                        'financiado',
                        'fecha_aps', 'fecha_baja', 'causa_baja', 'im_ingenieria', 'im_materiales', 'im_obracivil',
                        'im_trabajos', 'subvenciones_europeas', 'subvenciones_nacionales', 'subvenciones_prtr',
                        'avifauna',
                        'valor_auditado', 'valor_contabilidad', 'cuenta_contable', 'porcentaje_modificacion',
                        'motivacion', 'obra_id', 'identificador_baja',
                    ]

                    obra_id = O.GiscedataProjecteObraTiBt.search([('element_ti_id', '=', linia['id'])])

                    # Filtre d'obres finalitzades
                    linia_obra = ''
                    if obra_id:
                        data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                        if data_finalitzacio_data:
                            if data_finalitzacio_data.get('data_finalitzacio', False):
                                data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                                inici_any = '{}-01-01'.format(self.year)
                                fi_any = '{}-12-31'.format(self.year)
                                if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                    linia_obra = O.GiscedataProjecteObraTiBt.read(obra_id, fields_to_read_obra)[0]
                    else:
                        linia_obra = ''

                    # CAMPS OBRA
                    if linia_obra != '':
                        data_ip = convert_spanish_date(
                            linia_obra['fecha_aps'] if not linia_obra['fecha_baja'] and linia_obra[
                                'tipo_inversion'] != '1' else ''
                        )
                        identificador_baja = (
                            get_inst_name(linia_obra['identificador_baja']) if linia_obra['identificador_baja'] else ''
                        )
                        tipo_inversion = (linia_obra['tipo_inversion'] or '0') if not linia_obra['fecha_baja'] else '1'
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
                        cuenta_contable = linia_obra['cuenta_contable']
                        financiado = format_f(
                            100.0 - linia_obra.get('financiado', 0.0), 2
                        )
                        avifauna = int(linia_obra['avifauna'] == True)
                    else:
                        data_ip = ''
                        identificador_baja = ''
                        tipo_inversion = ''
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
                        financiado = ''

                    output = [
                        '{}{}'.format(self.prefix_BT, identificador_tramo),  # IDENTIFICADOR TRAMO
                        tram.get('cini', '') or '',  # CINI
                        codigo_ccuu or '',  # CODIGO_CCUU
                        nudo_inicial,  # ORIGEN
                        nudo_final,  # DESTINO
                        ccaa_1,  # CODIGO_CCAA_1
                        ccaa_2,  # CODIGO_CCAA_2
                        propiedad,  # PROPIEDAD
                        tension_explotacion,  # TENSION_EXPLOTACIÓN
                        tension_construccion,  # TENSION_CONSTRUCCIÓN
                        format_f(longitud, 3),  # LONGITUD
                        resistencia,  # RESISTENCIA
                        reactancia,  # REACTANCIA
                        intensitat,  # INTENSIDAD
                        punto_frontera,  # PUNTO_FRONTERA
                        modelo,  # MODELO
                        operacion,  # OPERACIÓN
                        fecha_aps,  # FECHA APS
                        causa_baja,  # CAUSA BAJA
                        fecha_baja or '',  # FECHA BAJA
                        data_ip,  # FECHA IP
                        tipo_inversion,  # TIPO INVERSION
                        im_ingenieria,  # IM_TRAMITES
                        im_construccion,  # IM_CONSTRUCCION
                        im_trabajos,  # IM_TRABAJOS
                        subvenciones_europeas,  # SUBVENCIONES EUROPEAS
                        subvenciones_nacionales,  # SUBVENCIONES NACIONALES
                        subvenciones_prtr,  # SUBVENCIONES PRTR
                        valor_auditado,  # VALOR AUDITADO
                        financiado,  # FINANCIADO
                        cuenta_contable,  # CUENTA CONTABLE
                        motivacion,  # MOTIVACION
                        avifauna,  # AVIFAUNA
                        identificador_baja,  # ID_BAJA
                    ]

                    self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
