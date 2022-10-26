#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
import math

from libcnmc.core import MultiprocessBased
from libcnmc.utils import (format_f, tallar_text, get_forced_elements, adapt_diff, format_f_6181, get_codi_actuacio,
                           convert_spanish_date, )
from libcnmc.models import F1Res4666


class LAT(MultiprocessBased):
    """
    Class that generates the LAT(1) file of  4666
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(LAT, self).__init__(**kwargs)
        self.extended = kwargs.get("extended", False)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies AT'
        self.report_name = 'CNMC INVENTARI AT'
        self.layer = 'LBT\_%'
        self.embarrats = kwargs.pop('embarrats', False)
        self.compare_field = kwargs["compare_field"]
        self.linia_tram_include = {}
        self.forced_ids = {}
        self.prefix = kwargs.pop('prefix', 'A') or 'A'
        self.dividir = kwargs.pop('dividir', False)

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

        search_params = [
            ('propietari', '=', True),
            ('name', '!=', 1)
        ]
        obj_lat = self.connection.GiscedataAtLinia
        ids = obj_lat.search(search_params, 0, 0, False, {'active_test': False})
        id_lat_emb = []
        if self.embarrats:
            id_lat_emb = obj_lat.search(
                [
                    ('name', '=', '1'),
                ], 0, 0, False, {'active_test': False})
        final_ids = ids + id_lat_emb

        self.forced_ids = get_forced_elements(self.connection, "giscedata.at.tram")
        data_tram = self.connection.GiscedataAtTram.read(
            self.forced_ids["include"],
            ["linia"]
        )

        for dt in data_tram:
            print(dt)
            if dt["linia"][0] not in self.linia_tram_include:
                self.linia_tram_include[dt["linia"][0]] = [dt["id"]]
            else:
                self.linia_tram_include[dt["linia"][0]].append([dt["id"]])

        return list(set(final_ids))

    def consumer(self):
        """
        Method that generates the csb file
        :return: List of arrays
        """
        O = self.connection

        def get_inst_name(element_id):
            vals = self.connection.GiscedataCts.read(
                element_id[0], ['name'])
            return vals['name']

        fields_to_read = [
            'baixa', 'data_pm', 'data_industria', 'coeficient', 'cini',
            'propietari', 'tensio_max_disseny_id', 'name', 'origen', 'final',
            'perc_financament', 'longitud_cad', 'cable',
            'tipus_instalacio_cnmc_id', 'data_baixa', self.compare_field,
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

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

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

                    # Agafem la tensió
                    if 'tensio_max_disseny_id' in tram and tram['tensio_max_disseny_id']:
                        if isinstance(tram['tensio_max_disseny_id'], (list, tuple)):
                            id_tensio = int(tram['tensio_max_disseny_id'][0])
                        else:
                            id_tensio = int(tram['tensio_max_disseny_id'])
                        tensio_aplicar = self.connection.GiscedataTensionsTensio.read(id_tensio, ["tensio"])["tensio"]
                        tensio = tensio_aplicar / 1000.0
                    elif 'tensio' in linia:
                        tensio = linia['tensio'] / 1000.0
                    else:
                        tensio = 0

                    comunitat = ''
                    if linia['municipi']:
                        ccaa_obj = O.ResComunitat_autonoma
                        id_comunitat = ccaa_obj.get_ccaa_from_municipi(
                            linia['municipi'][0])
                        comunidad = O.ResComunitat_autonoma.read(id_comunitat,
                                                                 ['codi'])
                        if comunidad:
                            comunitat = comunidad[0]['codi']

                    # Propietari
                    if tram.get('propietari', False):
                        propietari = tram['propietari']
                    else:
                        propietari = ''

                    # Tensión_explotación
                    if linia.get('tensio', False):
                        tension_explotacion = linia['tensio']
                    else:
                        tension_explotacion = ''

                    # Tensión_construcción
                    if tram.get('tensio_max_disseny_id', False):
                        tension_construccion = tram['tensio_max_disseny_id']
                    else:
                        tension_construccion = ''

                    # Resistencia, Reactancia, Intensitat
                    if tram.get('cable', False):
                        cable_obj = self.pool.get('giscedata.at.cables')
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
                        obra_id = tram['obra_id']
                        obra_at_obj = self.pool.get('giscedata.projecte.obra.ti.at')
                        causa_baja_data = obra_at_obj.read(obra_id, ['causa_baja'])
                        if causa_baja_data.get('causa_baja', False):
                            causa_baja = causa_baja_data['causa_baja']
                    else:
                        causa_baja = '0'

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

                    # Filtre d'obres finalitzades
                    data_finalitzacio = O.GiscedataProjecteObra.read(obra_id, ['data_finalitzacio'])
                    inici_any = '{}-01-01'.format(self.year)

                    if obra_id and data_finalitzacio >= inici_any:
                        linia = O.GiscedataProjecteObraTiCts.read(obra_id, fields_to_read_obra)[0]
                    else:
                        linia = ''

                    # CAMPS OBRA
                    if linia != '':
                        data_ip = convert_spanish_date(
                            linia['fecha_aps'] if not linia['fecha_baja'] and linia['tipo_inversion'] != '1' else ''
                        )
                        identificador_baja = (
                            get_inst_name(linia['identificador_baja']) if linia['identificador_baja'] else ''
                        )
                        tipo_inversion = (linia['tipo_inversion'] or '0') if not linia['fecha_baja'] else '1'
                        im_ingenieria = format_f_6181(linia['im_ingenieria'] or 0.0, float_type='euro')
                        im_materiales = format_f_6181(linia['im_materiales'] or 0.0, float_type='euro')
                        im_obracivil = format_f_6181(linia['im_obracivil'] or 0.0, float_type='euro')
                        im_construccion = str(format_f(
                            float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", "."))
                            , 2)).replace(".", ",")
                        im_trabajos = format_f_6181(linia['im_trabajos'] or 0.0, float_type='euro')
                        subvenciones_europeas = format_f_6181(linia['subvenciones_europeas'] or 0.0, float_type='euro')
                        subvenciones_nacionales = format_f_6181(linia['subvenciones_nacionales'] or 0.0,
                                                                float_type='euro')
                        subvenciones_prtr = format_f_6181(linia['subvenciones_prtr'] or 0.0, float_type='euro')
                        valor_auditado = format_f_6181(linia['valor_auditado'] or 0.0, float_type='euro')
                        motivacion = get_codi_actuacio(O, linia['motivacion'] and linia['motivacion'][0]) if not \
                            linia['fecha_baja'] else ''
                        cuenta_contable = linia['cuenta_contable']
                        financiado = format_f(
                            100.0 - linia.get('financiado', 0.0), 2
                        )
                        avifauna = int(linia['avifauna'] == True)
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
                            '{}{}'.format(self.prefix, tram['name']),  # IDENTIFICADOR
                            tram.get('cini', '') or '',         # CINI
                            codi_ccuu or '',                    # CODIGO_CCUU
                            origen or edge['start_node'][1],    # ORIGEN
                            final or edge['end_node'][1],       # DESTINO
                            comunitat,                          # CODIGO_CCAA_1
                            comunitat,                          # CODIGO_CCAA_2
                            propietari,                         # PROPIEDAD
                            tension_explotacion,                # TENSION_EXPLOTACIÓN
                            tension_construccion,               # TENSION_CONSTRUCCIÓN
                            format_f(100.0 - tram.get('perc_financament', 0.0), 2),  # FINANCIADO
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
                    if self.extended:
                        # S'ha especificat que es vol la versio extesa
                        if 'provincia' in linia:
                            provincia = O.ResCountryState.read(
                                linia['provincia'][0], ['name']
                            )
                            output.append(provincia.get('name', ""))
                        else:
                            output.append("")

                        if 'municipi' in linia:
                            municipi = O.ResMunicipi.read(
                                linia['municipi'][0], ['name']
                            )
                            output.append(municipi.get('name', ""))
                        else:
                            output.append("")

                    self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
