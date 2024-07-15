# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
from multiprocessing import Manager
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP, \
    TARIFAS_AT, TARIFAS_BT
from libcnmc.utils import get_ine, get_comptador, format_f, get_srid,\
    convert_srid, get_tipus_connexio
from libcnmc.core import StopMultiprocessBased
from ast import literal_eval
import logging
from shapely import wkt


class FA1(StopMultiprocessBased):
    def __init__(self, **kwargs):
        """
        FA1 class constructor

        :param codi_r1: R1 code of the company
        :type codi_r1: str
        :param year: Year of the resolution
        :type year: int
        """

        super(FA1, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        manager = Manager()
        self.cts = manager.dict()
        self.cnaes = manager.dict()
        self.base_object = 'CUPS'
        self.report_name = 'FA1 - CUPS'
        self.reducir_cups = kwargs.get("reducir_cups", False)
        self.allow_cna = kwargs.get("allow_cna", False)
        self.zona_qualitat = kwargs.get("zona_qualitat", "ct")
        self.layer = 'LBT\_%'
        self.ultim_dia_any = '{}-12-31'.format(self.year)
        id_res_like = self.connection.ResConfig.search(
            [('name', '=', 'giscegis_btlike_layer')]
        )
        mod_all_year = self.connection.GiscedataPolissaModcontractual.search(
            [
                ("data_inici", "<=", "{}-01-01".format(self.year)),
                ("data_final", ">=", "{}-12-31".format(self.year)),
                ("tarifa.name", 'not ilike', '%RE%'),
                ('polissa_id.state', 'in', ['tall', 'activa', 'baixa'])
            ], 0, 0, False, {"active_test": False}
        )
        mods_ini = self.connection.GiscedataPolissaModcontractual.search(
            [
                ("data_inici", ">=", "{}-01-01".format(self.year)),
                ("data_inici", "<=", "{}-12-31".format(self.year)),
                ("tarifa.name", 'not ilike', '%RE%'),
                ('polissa_id.state', 'in', ['tall', 'activa', 'baixa'])
            ], 0, 0, False, {"active_test": False}
        )
        mods_fi = self.connection.GiscedataPolissaModcontractual.search(
            [
                ("data_final", ">=", "{}-01-01".format(self.year)),
                ("data_final", "<=", "{}-12-31".format(self.year)),
                ("tarifa.name", 'not ilike', '%RE%'),
                ('polissa_id.state', 'in', ['tall', 'activa', 'baixa'])
            ], 0, 0, False, {"active_test": False}
        )

        self.generate_derechos = kwargs.pop("derechos", False)

        self.modcons_in_year = set(mods_fi + mods_ini + mod_all_year)

        if id_res_like:
            self.layer = self.connection.ResConfig.read(
                id_res_like, ['value']
            )[0]['value']
        mod_all_year = self.connection.GiscedataPolissaModcontractual.search(
            [
                ("data_inici", "<=", "{}-01-01".format(self.year)),
                ("data_final", ">=", "{}-12-31".format(self.year)),
                ("tarifa.name", 'not ilike', '%RE%'),
                ('polissa_id.state', 'in', ['tall', 'activa', 'baixa'])
            ], 0, 0, False, {"active_test": False}
        )
        mods_ini = self.connection.GiscedataPolissaModcontractual.search(
            [
                ("data_inici", ">=", "{}-01-01".format(self.year)),
                ("data_inici", "<=", "{}-12-31".format(self.year)),
                ("tarifa.name", 'not ilike', '%RE%'),
                ('polissa_id.state', 'in', ['tall', 'activa', 'baixa'])
            ], 0, 0, False, {"active_test": False}
        )
        mods_fi = self.connection.GiscedataPolissaModcontractual.search(
            [
                ("data_final", ">=", "{}-01-01".format(self.year)),
                ("data_final", "<=", "{}-12-31".format(self.year)),
                ("tarifa.name", 'not ilike', '%RE%'),
                ('polissa_id.state', 'in', ['tall', 'activa', 'baixa'])
            ], 0, 0, False, {"active_test": False}
        )
        self.modcons_in_year = set(mods_fi + mods_ini + mod_all_year)
        self.default_o_cod_tfa = None
        self.default_o_cnae = None
        search_params = [
            ('name', '=', 'libcnmc_4_2015_default_f1')
        ]
        id_config = self.connection.ResConfig.search(
            search_params
        )

        self.generate_derechos = kwargs.pop("derechos", False)

        if id_config:
            config = self.connection.ResConfig.read(id_config[0], [])
            default_values = literal_eval(config['value'])
            if default_values.get('o_cod_tfa'):
                self.default_o_cod_tfa = default_values.get('o_cod_tfa')
            if default_values.get('o_cnae'):
                self.default_o_cnae = default_values.get('o_cnae')

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

    def get_codi_tarifa(self, codi_tarifa):
        """
        Returns the codi tarifa of the polissa

        :param codi_tarifa: ERP codi tarifa
        :return: The codi tarifa for the FA1
        :rtype: str
        """

        return CODIS_TARIFA.get(codi_tarifa, '')

    def get_derechos(self, tarifas, years):
        """
        Returns a list of CUPS with derechos

        :param tarifas: Lis of tarifas of the polissas that are in the CUPS
        :param years: Number of years to search back
        :return: List of ids of the
        """

        O = self.connection

        polisses_baixa_id = O.GiscedataPolissa.search(
            [
                ("data_baixa", "<=", "{}-12-31".format(self.year - 1)),
                ("data_baixa", ">", "{}-01-01".format(self.year - years)),
                ("tarifa", "in", tarifas)
            ],
            0, 0, False, {'active_test': False}
        )

        cups_polisses_baixa = [x["cups"][0] for x in O.GiscedataPolissa.read(
            polisses_baixa_id, ["cups"]
        )]

        cups_derechos = O.GiscedataCupsPs.search(
            [
                ("id", "in", cups_polisses_baixa),
                ("polissa_polissa", "=", False)
            ]
        )

        polissa_eliminar_id = O.GiscedataPolissaModcontractual.search(
            [
                ("cups", "in", cups_derechos),
                '|', ("data_inici", ">", "{}-01-01".format(self.year)),
                ("data_final", ">", "{}-01-01".format(self.year))
            ],
            0, 0, False, {'active_test': False}
        )

        cups_eliminar_id = [x["cups"][0] for x in
                            O.GiscedataPolissaModcontractual.read(
                                polissa_eliminar_id, ["cups"]
                            )]

        cups_derechos = list(set(cups_derechos) - set(cups_eliminar_id))

        return cups_derechos

    def get_sequence(self):
        """
        Generates the list of cups to generate the FA1

        :return: List of CUPS
        :rtype: list[int]
        """
        data_ini = '%s-01-01' % (self.year + 1)
        search_params = [('active', '=', True),
                         '|',
                         ('create_date', '<', data_ini),
                         ('create_date', '=', False)]

        ret_cups_ids = self.connection.GiscedataCupsPs.search(
            search_params, 0, 0, False, {'active_test': False})

        ret_cups_tmp = self.connection.GiscedataCupsPs.read(
            ret_cups_ids, ["polisses"]
        )
        ret_cups = []

        for cups in ret_cups_tmp:
            if set(cups['polisses']).intersection(self.modcons_in_year):
                ret_cups.append(cups["id"])
        if self.generate_derechos:
            cups_derechos_bt = self.get_derechos(TARIFAS_BT, 2)
            cups_derechos_at = self.get_derechos(TARIFAS_AT, 4)
            return list(set(ret_cups + cups_derechos_at + cups_derechos_bt))
        else:
            return ret_cups

    def get_polissa(self, cups_id):
        polissa_obj = self.connection.GiscedataPolissa
        context = {
            'date': self.ultim_dia_any,
            'active_test': False
        }
        polissa_id = polissa_obj.search([
            ('cups', '=', cups_id),
            ('state', 'in', ['tall', 'activa', 'baixa']),
            ('data_alta', '<=', self.ultim_dia_any),
        ], 0, 1, 'data_alta desc', context)
        return polissa_id

    def get_zona_qualitat(self, tipus_zona, codi_ct, id_municipi):
        zona_q = False
        if tipus_zona == 'ct':
            if codi_ct:
                zona_q = self.get_zona_qualitat_ct(codi_ct)
        elif tipus_zona == 'municipi':
            if id_municipi:
                zona_q = self.get_zona_qualitat_municipi(id_municipi)
        return zona_q

    def get_zona_qualitat_ct(self, codi_ct):
        """
        Returns the quality zone of a given ct
        :param codi_ct: Codi CT
        :type codi_ct: str
        :return: Quality zone
        :rtype: str
        """
        zona_qualitat = ''
        if codi_ct:
            if codi_ct in self.cts:
                return self.cts[codi_ct]
            else:
                ct_ids = self.connection.GiscedataCts.search(
                    [('name', '=', codi_ct)],
                    0, 0, False, {"active_test": False}
                )

                if ct_ids:
                    dades_ct = self.connection.GiscedataCts.read(
                        ct_ids[0], ['zona_id'])
                    if dades_ct['zona_id']:
                        zona_desc = dades_ct['zona_id'][1].upper().replace(
                            ' ', ''
                        )
                        if zona_desc in CODIS_ZONA:
                            zona_qualitat = CODIS_ZONA[zona_desc]
                            self.cts[codi_ct] = zona_qualitat
        return zona_qualitat

    def get_zona_qualitat_municipi(self, id_municipi):
        """
        Returns the quality zone of a given municipi
        :param id_municipi: identificador del municipi
        :type id_municipi: int
        :return: Quality zone
        :rtype: str
        """
        conn = self.connection
        zona_qualitat = ''

        if id_municipi:
            zona = conn.ResMunicipi.read(id_municipi[0], ["zona"])
            if zona.get('zona'):
                zona_desc = zona.get('zona')[1].upper().replace(' ', '')
                if zona_desc in CODIS_ZONA:
                    zona_qualitat = CODIS_ZONA[zona_desc]
        return zona_qualitat

    def get_comptador_cini(self, polissa_id):
        comp_obj = self.connection.GiscedataLecturesComptador
        cid = get_comptador(self.connection, polissa_id, self.year)
        cini = ''
        if cid:
            comptador = comp_obj.read(cid[0], ['cini'])
            cini = comptador['cini'] or ''
        return cini

    def get_data_comptador(self, polissa_id):
        comp_obj = self.connection.GiscedataLecturesComptador
        comp_id = get_comptador(self.connection, polissa_id, self.year)
        o_fecha_ins = ''
        if comp_id:
            comp_id = comp_id[0]
            comp = comp_obj.read(comp_id, ['data_alta'])
            data = comp['data_alta']
            data_format = datetime.strptime(str(data), '%Y-%m-%d')
            o_fecha_ins = data_format.strftime('%d/%m/%Y')
        return o_fecha_ins

    def get_cambio_titularidad(self, cups_id):
        O = self.connection
        intervals = O.GiscedataCupsPs.get_modcontractual_intervals
        start = '%s-01-01' % self.year
        end = '%s-12-31' % self.year
        modcons = intervals(cups_id, start, end, {'ffields': ['titular']})
        if len(modcons) > 1:
            return '1'
        else:
            return '0'

    def get_baixa_cups(self, cups_id):
        """
        Devuelve si un CUPS ha estado de baja durante el año

        :param cups_id: Id del
        :param cups_id: int
        :return: Si ha estado de baja durante el año devuelve 0 ,sino 1
        :rtype: int
        """

        O = self.connection
        polissa_obj = O.GiscedataPolissa

        data_inici = '%s-01-01' % self.year
        data_fi = '%s-12-31' % self.year

        # Busquem pòlisses que s'han donat de baixa durant l'any
        search_params = [
            ('cups', '=', cups_id),
            ('data_baixa', '>=', data_inici),
            ('data_baixa', '<', data_fi),
            ('state', 'in', ['tall', 'activa', 'baixa']),
        ]
        polisses_baixa_ids = polissa_obj.search(
            search_params, 0, False, False, {'active_test': False})

        # Agafem les dates de baixa de les pòlisses
        data_format = '%Y-%m-%d'
        dates_baixa = [
            polissa['data_baixa'] for polissa
            in polissa_obj.read(polisses_baixa_ids, ['data_baixa'])
        ]

        # Sumem un dia a cada data de baixa
        dates_alta = [
            (
                datetime.strptime(data_baixa, data_format) + timedelta(days=1)
            ).strftime(data_format)
            for data_baixa in dates_baixa
        ]

        # Busquem pòlisses on la data d'alta estigui en les dates d'alta
        polisses_alta_ids = polissa_obj.search(
            [('cups', '=', cups_id),
             ('data_alta', 'in', dates_alta),
             ('state', 'in', ['tall', 'activa', 'baixa']),
             ],
            0, False, False, {'active_test': False}
        )

        # Si hi ha el mateix número de pòlisses que s'han donat de baixa durant
        # l'any i pòlisses que s'han donat d'alta al dia següent llavors '0',
        # altrament '1'
        if len(polisses_baixa_ids) == len(polisses_alta_ids):
            return '0'
        else:
            return '1'

    def consumer(self):
        """
        Consumer function to generate FA1

        :return: None
        """

        O = self.connection
        ultim_dia_any = '%s-12-31' % self.year
        search_glob = [
            ('state', 'in', ['tall', 'activa', 'baixa']),
            ('data_alta', '<=', ultim_dia_any),
            '|',
            ('data_baixa', '>=', ultim_dia_any),
            ('data_baixa', '=', False)
        ]
        context_glob = {'date': ultim_dia_any, 'active_test': False}
        zq = self.zona_qualitat
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                fields_to_read = [
                    'name', 'id_escomesa', 'id_municipi', 'cne_anual_activa',
                    'cne_anual_reactiva', 'cnmc_potencia_facturada', 'et',
                    'polisses', 'potencia_conveni', 'potencia_adscrita',
                    'autoconsum_id', 'cnmc_numero_lectures',
                    'cnmc_factures_estimades', 'cnmc_factures_total',
                    'cnmc_energia_autoconsumida', 'cnmc_energia_excedentaria',
                    'force_potencia_adscrita', 'cnmc_conexion_autoconsumo',
                ]
                cups = O.GiscedataCupsPs.read(item, fields_to_read)
                if not cups or not cups.get('name'):
                    self.input_q.task_done()
                    continue
                if self.reducir_cups:
                    o_name = cups['name'][:20]
                else:
                    o_name = cups['name'][:22]

                # FACTURAS ESTIMADAS
                o_facturas_estimadas = 0
                if cups.get('cnmc_factures_estimades', False):
                    o_facturas_estimadas = cups['cnmc_factures_estimades']

                # FACTURAS TOTAL
                o_facturas_total = 0
                if cups.get('cnmc_factures_total', False):
                    o_facturas_total = cups['cnmc_factures_total']

                # ENERGIA_AUTOCONSUMIDA
                o_energia_autoconsumida = ''
                if cups.get('cnmc_energia_autoconsumida', False):
                    o_energia_autoconsumida = cups['cnmc_energia_autoconsumida']

                # ENERGIA_EXCEDENTARIA
                o_energia_excedentaria = ''
                if cups.get('cnmc_energia_excedentaria', False):
                    o_energia_excedentaria = abs(cups['cnmc_energia_excedentaria'])

                # AUTOCONSUMO, CAU, COD_AUTO, COD_GENERACION_AUTO I CONEXION_AUTOCONSUMO
                o_autoconsumo = 0
                o_cau = ''
                o_cod_auto = ''
                o_cod_generacio_auto = ''
                o_conexion_autoconsumo = ''

                cups_obj = O.GiscedataCupsPs
                autoconsum_id_data = cups_obj.get_autoconsum_on_date(item, ultim_dia_any)

                if autoconsum_id_data:
                    # AUTOCONSUMO
                    autoconsum_id = autoconsum_id_data[0]
                    autoconsum_data = O.GiscedataAutoconsum.read(autoconsum_id, [
                        'cau', 'tipus_autoconsum', 'generador_id', 'codi_cnmc',
                        'collectiu',
                    ])
                    o_autoconsumo = autoconsum_data['collectiu'] and 2 or 1
                    # COD_AUTO
                    if autoconsum_data.get('codi_cnmc', False):
                        o_cod_auto = autoconsum_data['codi_cnmc']

                    # CAU
                    if autoconsum_data.get('cau', False):
                        o_cau = autoconsum_data['cau']

                    # COD_GENERACION_AUTO
                    if autoconsum_data.get('generador_id', False):
                        generador_id = autoconsum_data['generador_id'][0]
                        generador_data = O.GiscedataAutoconsumGenerador.read(generador_id, ['cini'])
                        if generador_data.get('cini', False):
                            cini = generador_data['cini']
                            o_cod_generacio_auto = cini[4]

                    # CONEXION_AUTOCONSUMO
                    if cups.get('cnmc_conexion_autoconsumo', False):
                        o_conexion_autoconsumo = cups['cnmc_conexion_autoconsumo']

                o_codi_ine_mun = ''
                o_codi_ine_prov = ''
                o_zona = ''
                o_potencia_facturada = format_f(
                    cups['cnmc_potencia_facturada'], 3) or ''
                if self.zona_qualitat:
                    o_zona = self.get_zona_qualitat(self.zona_qualitat, cups['et'], cups['id_municipi'])
                if cups['id_municipi']:
                    id_mun = cups["id_municipi"][0]
                    o_codi_ine_prov, o_codi_ine_mun = self.get_ine(id_mun)
                o_nom_node = ''
                o_tensio = ''
                o_connexio = ''
                vertex = False
                if cups and cups['id_escomesa']:
                    o_connexio = get_tipus_connexio(
                        O, cups['id_escomesa'][0]
                    )
                    search_params = [('escomesa', '=', cups['id_escomesa'][0])]
                    bloc_escomesa_id = O.GiscegisBlocsEscomeses.search(
                        search_params
                    )
                    id_escomesa = cups.get("id_escomesa")
                    vertex = ''
                    if id_escomesa:
                        escomesa = O.GiscedataCupsEscomesa.read(id_escomesa[0], ["node_id", "geom"])
                        if escomesa.get("geom",False):
                            geom = wkt.loads(escomesa["geom"]).coords[0]
                            vertex = {"x":geom[0], "y": geom[1]}
                        if escomesa.get("node_id", False):
                            o_nom_node = escomesa.get("node_id")[1]
                    if bloc_escomesa_id and not o_nom_node:
                        bloc_escomesa = O.GiscegisBlocsEscomeses.read(
                            bloc_escomesa_id[0], ['node', 'vertex']
                        )
                        if bloc_escomesa['vertex'] and not vertex:
                            vertex = O.GiscegisVertex.read(
                                bloc_escomesa['vertex'][0], ['x', 'y']
                            )
                            if bloc_escomesa['node']:
                                node = O.GiscegisNodes.read(
                                    [bloc_escomesa['node'][0]], ['name'])
                                o_nom_node = node[0]['name']
                o_nom_node = o_nom_node.replace('*', '')
                search_params = [('cups', '=', cups['id'])] + search_glob
                polissa_id = O.GiscedataPolissa.search(
                    search_params, 0, 1, 'data_alta desc', context_glob)

                o_potencia = 0
                o_cnae = ''
                o_cod_tfa = ''

                # energies consumides
                o_anual_activa = format_f(
                    cups['cne_anual_activa'] or 0.0, decimals=3)
                o_anual_reactiva = format_f(
                    cups['cne_anual_reactiva'] or 0.0, decimals=3)

                # CINI_EQUIPO_MEDIDA / FECHA_INSTALACION
                polissa_id_equipos = self.get_polissa(cups['id'])
                if polissa_id_equipos:
                    polissa_id_equipos = polissa_id_equipos[0]
                    o_comptador_cini = self.get_comptador_cini(polissa_id_equipos)
                    o_comptador_data = self.get_data_comptador(polissa_id_equipos)
                else:
                    o_comptador_cini = ''
                    o_comptador_data = ''

                if polissa_id:
                    fields_to_read = [
                        'potencia', 'cnae', 'tarifa', 'butlletins'#, 'tensio'
                    ]
                    polissa_id = polissa_id[0]
                    polissa = O.GiscedataPolissa.read(
                        polissa_id, fields_to_read, context_glob
                    )
                    o_potencia = polissa['potencia']
                    if polissa['cnae']:
                        cnae_id = polissa['cnae'][0]
                        if cnae_id in self.cnaes:
                            o_cnae = self.cnaes[cnae_id]
                        else:
                            o_cnae = O.GiscemiscCnae.read(
                                cnae_id, ['name']
                            )['name']
                            self.cnaes[cnae_id] = o_cnae
                    else:
                        try:
                            polissa_act = O.GiscedataPolissa.read(
                                polissa_id, fields_to_read
                            )
                            cnae_id = polissa_act['cnae'][0]
                            if cnae_id in self.cnaes:
                                o_cnae = self.cnaes[cnae_id]
                            else:
                                o_cnae = O.GiscemiscCnae.read(
                                    cnae_id, ['name']
                                )['name']
                                self.cnaes[cnae_id] = o_cnae
                        except:
                            pass
                    # Si som aqui es perque tenim polissa activa a dia 31/12/18
                    comptador_actiu = get_comptador(
                        self.connection, polissa['id'], self.year
                    )
                    if comptador_actiu:
                        comptador_actiu = comptador_actiu[0]
                        comptador = O.GiscedataLecturesComptador.read(
                            comptador_actiu, ['cini', 'tg']
                        )
                        if not comptador['cini']:
                            comptador['cini'] = ''

                    else:
                        if self.raven:
                            self.raven.captureMessage(
                                "Missing Comptador on Polissa with ID {}".format(
                                    polissa['id']
                                ),
                                level=logging.WARNING
                            )

                    if polissa['tarifa']:
                        o_cod_tfa = self.get_codi_tarifa(polissa['tarifa'][1])

                    # ESTADO CONTRATO
                    contracte_obj = O.GiscedataPolissaModcontractual
                    date_mod = '{}-12-31'.format(self.year)
                    search_params = [('data_inici', '<=', date_mod),
                                     ('data_final', '>=', date_mod),
                                     ('polissa_id', '=', polissa_id)]
                    modcon_estat_id = contracte_obj.search(search_params, 0, 0, False, {'active_test': False})
                    if modcon_estat_id:
                        o_estat_contracte = 0
                    else:
                        o_estat_contracte = 1

                else:
                    o_estat_contracte = 1

                    search_modcon = [
                        ('id', 'in', cups['polisses']),
                        ('data_inici', '<=', ultim_dia_any),
                        ('polissa_id.state', 'in', ['tall', 'activa', 'baixa'])
                    ]
                    modcons = None
                    if len(cups['polisses']):
                        modcons = O.GiscedataPolissaModcontractual.search(
                            search_modcon, 0, 1, 'data_inici desc'
                            , {'active_test': False})
                    if modcons:
                        modcon_id = modcons[0]

                        fields_to_read_modcon = [
                            'cnae',
                            'tarifa',
                            'tensio',
                            'potencia',
                            'polissa_id',
                            'data_final'
                        ]

                        modcon = O.GiscedataPolissaModcontractual.read(
                            modcon_id, fields_to_read_modcon)

                        if modcon['tarifa']:
                            o_cod_tfa = self.get_codi_tarifa(
                                modcon['tarifa'][1]
                            )
                        if modcon['cnae']:
                            cnae_id = modcon['cnae'][0]
                            if cnae_id in self.cnaes:
                                o_cnae = self.cnaes[cnae_id]
                            else:
                                o_cnae = O.GiscemiscCnae.read(
                                    cnae_id, ['name']
                                )['name']
                                self.cnaes[cnae_id] = o_cnae
                        if self.tensio_modcon and modcon.get('tensio'):
                                o_tensio = float(modcon['tensio'])
                        if modcon['potencia']:
                            o_potencia = modcon['potencia']
                    else:
                        # No existeix modificació contractual per el CUPS
                        o_potencia = cups['potencia_conveni']
                        if self.default_o_cnae:
                            o_cnae = self.default_o_cnae
                        if self.default_o_cod_tfa:
                            o_cod_tfa = self.default_o_cod_tfa
                # Tensió d'alimentació en kV
                if not o_tensio:
                    if cups.get('id_escomesa', False):
                        search_params = [
                            ('escomesa', '=', cups['id_escomesa'][0])
                        ]
                        id_esc_gis = O.GiscegisEscomesesTraceability.search(
                            search_params
                        )

                        if id_esc_gis:
                            tensio_gis = O.GiscegisEscomesesTraceability.read(
                                id_esc_gis, ['tensio']
                            )[0]['tensio']
                            o_tensio = float(tensio_gis)
                else:
                    o_tensio = ''

                # potencia adscrita
                o_pot_ads = 0

                if cups['force_potencia_adscrita']:
                    o_pot_ads = cups['potencia_adscrita']
                else:
                    # Buscar butlletins vigents en data circular i agafgar el màxim
                    current_date = '{}-01-01'.format(self.year)
                    but_ids = self.connection.GiscedataButlleti.search([
                        ('cups_id', '=', cups['id']),
                        '|', '|', '|',
                        '&', ('data', '=', False), ('data_vigencia', '=', False),
                        '&', ('data', '<=', current_date), ('data_vigencia', '=', False),
                        '&', ('data', '=', False), ('data_vigencia', '>=', current_date),
                        '&', ('data', '<=', current_date), ('data_vigencia', '>=', current_date),
                    ], 0, 0, False, {'active_test': False})
                    if but_ids:
                        o_pot_ads = max(
                            b['pot_max_admisible']
                            for b in self.connection.GiscedataButlleti.read(but_ids, ['pot_max_admisible'])
                        )
                if o_pot_ads < o_potencia:
                    o_pot_ads = o_potencia

                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(
                        get_srid(O), (vertex['x'], vertex['y'])
                    )


                o_num_lectures = format_f(
                    cups['cnmc_numero_lectures'], decimals=3) or '0'
                o_titular = self.get_cambio_titularidad(cups['id'])
                o_baixa = self.get_baixa_cups(cups['id'])

                self.output_q.put([
                    o_nom_node,                                         # Nudo
                    format_f(res_srid[0], decimals=3),                  # X
                    format_f(res_srid[1], decimals=3),                  # Y
                    '0,000',                                            # Z
                    o_cnae,                                             # CNAE
                    o_cod_tfa,                                          # Codigo de tarifa
                    o_name,                                             # CUPS
                    o_codi_ine_mun,                                     # Municipio
                    o_codi_ine_prov,                                    # Provincia
                    o_zona,                                             # Zona de calidad
                    o_connexio,                                         # Conexion
                    format_f(o_tensio/1000.0, decimals=3),              # Tension de alimentacion
                    o_estat_contracte,                                  # Estado de contrato
                    format_f(o_potencia or '0,000', decimals=3),        # Potencia contratada
                    format_f(o_pot_ads, decimals=3),                    # Potencia adscrita a la instalacion
                    format_f(o_anual_activa, decimals=3),               # Energia activa anual consumida
                    format_f(o_anual_reactiva, decimals=3),             # Energia reactiva anual consumida
                    o_autoconsumo,                                      # Autoconsumo si o no
                    o_comptador_cini,                                   # CINI
                    o_comptador_data,                                   # INSTALACION
                    o_num_lectures,                                     # NUMERO LECTURAS
                    o_baixa,                                            # BAJA SUMINISTRO
                    o_titular,                                          # CAMBIO TITULARIDAD
                    o_facturas_estimadas,                               # fff
                    o_facturas_total,                                   # fff
                    o_cau,                                              # fff
                    o_cod_auto,                                         # fff
                    o_cod_generacio_auto,                               # fff
                    o_conexion_autoconsumo,                             # fff
                    format_f(o_energia_autoconsumida, decimals=3),      # fff
                    format_f(o_energia_excedentaria, decimals=3)        # fff
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
