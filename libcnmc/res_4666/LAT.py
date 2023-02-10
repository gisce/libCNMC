#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
import math

from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f, tallar_text, get_forced_elements, adapt_diff
from libcnmc.models import F1Res4666


class LAT(StopMultiprocessBased):
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
        fields_to_read = [
            'baixa', 'data_pm', 'data_industria', 'coeficient', 'cini',
            'propietari', 'tensio_max_disseny_id', 'name', 'origen', 'final',
            'perc_financament', 'circuits', 'longitud_cad', 'cable',
            'tipus_instalacio_cnmc_id', 'data_baixa', self.compare_field,
            'baixa', 'data_baixa', 'conductors', 'id_regulatori'
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
                if item == "STOP":
                    self.input_q.task_done()
                    break
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

                    # Si hi ha 'id_regulatori' el posem
                    if tram.get('id_regulatori', False):
                        o_tram = tram['id_regulatori']
                    else:
                        o_tram = '{}{}'.format(self.prefix, tram['name'])

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

                    # Agafem el cable de la linia
                    if 'cable' in tram:
                        cable = O.GiscedataAtCables.read(
                            tram['cable'][0], ['intensitat_admisible',
                                               'seccio'])
                    else:
                        cable = O.GiscedataAtCables.read(
                            id_desconegut[0], ['tipus'])

                    # Capacitat
                    if 'intensitat_admisible' in cable:
                        cap = (cable['intensitat_admisible'] * tensio *
                               math.sqrt(3) / 1000.0)
                    else:
                        cap = 0

                    if cap < 1:
                        capacitat = 1
                    else:
                        capacitat = int(round(cap))

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

                    if tram[self.compare_field]:
                        data_entregada = tram[self.compare_field]
                        entregada = F1Res4666(**data_entregada)
                        if tram['tipus_instalacio_cnmc_id']:
                            id_ti = tram['tipus_instalacio_cnmc_id'][0]
                            ti = O.GiscedataTipusInstallacio.read(
                                id_ti,
                                ['name'])['name']
                        else:
                            ti = ''
                        actual = F1Res4666(
                            '{}{}'.format(self.prefix, tram['name']),
                            tram['cini'],
                            tram['origen'],
                            tram['final'],
                            ti,
                            comunitat,
                            comunitat,
                            format_f(
                                100.0 - tram.get('perc_financament', 0.0), 2
                            ),
                            data_pm,
                            data_baixa,
                            tram.get('circuits', 1) or 1,
                            1,
                            tensio,
                            format_f(longitud, 3),
                            format_f(cable.get('intensitat_admisible', 0) or 0),
                            format_f(float(cable.get('seccio', 0)), 2),
                            str(capacitat),
                            0
                        )
                        if actual == entregada and fecha_baja == '':
                            estado = 0
                        else:
                            self.output_m.put("{} {}".format(tram["name"], adapt_diff(actual.diff(entregada))))
                            estado = 1
                    else:
                        if tram['data_pm']:
                            if tram['data_pm'][:4] != str(self.year):
                                self.output_m.put("Identificador:{} No estava en el fitxer carregat al any n-1 i la data de PM es diferent al any actual".format(tram["name"]))
                                estado = '1'
                            else:
                                estado = '2'
                        else:
                            self.output_m.put("Identificador:{} No estava en el fitxer carregat al any n-1".format(tram["name"]))
                            estado = '1'
                    if tram['conductors']:
                        conductors = tram['conductors']
                    else:
                        conductors = 1
                    output = [
                        o_tram,  # IDENTIFIC.
                        tram.get('cini', '') or '',         # CINI
                        origen or edge['start_node'][1],    # ORIGEN
                        final or edge['end_node'][1],       # DESTINO
                        codi_ccuu or '',                    # CODIGO_CCUU
                        comunitat,                          # CODIGO_CCAA_1
                        comunitat,                          # CODIGO_CCAA_2
                        format_f(
                            100.0 - tram.get('perc_financament', 0.0), 2
                        ),                                  # FINANCIADO
                        data_pm,                            # FECHA APS
                        fecha_baja or '',                   # FECHA BAJA
                        tram.get('circuits', 1) or 1,       # NUMERO_CIRCUITOS
                        conductors,                         # NUMERO_CONDUCTORES
                        format_f(tensio, 3),                # NIVEL TENSION
                        format_f(longitud, 3),              # LONGITUD
                        format_f(cable.get('intensitat_admisible', 0) or 0),    # INTENSIDAD MAXIMA
                        format_f(cable.get('seccio', 0) or 0, 3),   # SECCION
                        capacitat,                          # CAPACIDAD
                        estado                              # ESTADO
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
                    self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
