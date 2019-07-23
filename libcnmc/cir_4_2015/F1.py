# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import re
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP, \
    TARIFAS_AT, TARIFAS_BT
from libcnmc.utils import get_ine, get_comptador, format_f, get_srid,\
    convert_srid
from libcnmc.core import MultiprocessBased
from ast import literal_eval
import logging
from shapely import wkt


class F1(MultiprocessBased):
    def __init__(self, **kwargs):
        """
        F1 class constructor

        :param codi_r1: R1 code of the company
        :type codi_r1: str
        :param year: Year of the resolution
        :type year: int
        """

        super(F1, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        manager = Manager()
        self.cts = manager.dict()
        self.cnaes = manager.dict()
        self.base_object = 'CUPS'
        self.report_name = 'F1 - CUPS'
        self.reducir_cups = kwargs.get("reducir_cups", False)
        self.allow_cna = kwargs.get("allow_cna", False)
        self.zona_qualitat = kwargs.get("zona_qualitat", "ct")
        self.layer = 'LBT\_%'
        id_res_like = self.connection.ResConfig.search(
            [('name', '=', 'giscegis_btlike_layer')]
        )
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
        :return: The codi tarifa for the F1
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
        Generates the list of cups to generate the F1

        :return: List of CUPS
        :rtype: list[int]
        """
        data_ini = '%s-01-01' % (self.year + 1)
        ret_cups_ids = self.connection.GiscedataCupsPs.search([])

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

    def get_tipus_connexio(self, id_escomesa):
        """
        Gets the tipus of connexio of an escomesa. If it's a BT escomesa we
        check the TramBT that suplies it to see if its aerial or underground.
        If it's not a BT escomesa we directly set the tipus of connexio to
        aerial.
        :param id_escomesa: Id of the escomesa
        :type id_escomesa: int
        :return: A or S depending on if the linia that suplies the escomesa
                 is aerial or underground
        :rtype: str
        """

        o = self.connection
        tipus = 'A'
        if 'node_id' in o.GiscedataCupsEscomesa.fields_get().keys() and 'edge_id' in o.GiscedataBtElement.fields_get().keys():
            node_id = o.GiscedataCupsEscomesa.read(
                id_escomesa, ['node_id']
            )['node_id']
            if node_id:
                edge_id = o.GiscegisEdge.search(
                    [
                        '|',
                        ('start_node', '=', node_id[0]),
                        ('end_node', '=', node_id[0])
                    ]
                )
                if edge_id:
                    tram_bt = o.GiscedataBtElement.search(
                        [('edge_id', '=', edge_id[0])]
                    )
                    if tram_bt:
                        tram_bt = o.GiscedataBtElement.read(
                            tram_bt[0], ['tipus_linia']
                        )
                        if tram_bt['tipus_linia']:
                            tipus = tram_bt['tipus_linia'][1][0]
        else:
            bloc = o.GiscegisBlocsEscomeses.search(
                [('escomesa', '=', id_escomesa)]
            )
            if bloc:
                bloc = o.GiscegisBlocsEscomeses.read(bloc[0], ['node'])
                if bloc['node']:
                    node = bloc['node'][0]
                    edge_bt = o.GiscegisEdge.search(
                        [
                            '|',
                            ('start_node', '=', node),
                            ('end_node', '=', node),
                            '|',
                            ('layer', 'ilike', self.layer),
                            ('layer', 'ilike', 'EMBARRA%BT%')
                        ]
                    )
                    edge = o.GiscegisEdge.read(
                        edge_bt[0], ['id_linktemplate']
                    )
                    if edge['id_linktemplate']:
                        tram_bt = o.GiscedataBtElement.search(
                            [
                                ('name', '=', edge['id_linktemplate'])
                            ]
                        )
                        if tram_bt:
                            tram_bt = o.GiscedataBtElement.read(
                                tram_bt[0], ['tipus_linia']
                            )
                            if tram_bt:
                                if tram_bt['tipus_linia']:
                                    tipus = tram_bt['tipus_linia'][1][0]

        return tipus

    def consumer(self):
        """
        Consumer function to generate F1

        :return: None
        """

        o_codi_r1 = 'R1-%s' % self.codi_r1[-3:]
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
                self.progress_q.put(item)
                fields_to_read = [
                    'name', 'id_escomesa', 'id_municipi', 'cne_anual_activa',
                    'cne_anual_reactiva', 'cnmc_potencia_facturada', 'et',
                    'polisses', 'potencia_conveni', 'potencia_adscrita',
                    "node_id"
                ]
                cups = O.GiscedataCupsPs.read(item, fields_to_read)
                if not cups or not cups.get('name'):
                    self.input_q.task_done()
                    continue
                if self.reducir_cups:
                    o_name = cups['name'][:20]
                else:
                    o_name = cups['name'][:22]
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
                if cups and cups['id_escomesa'] and "node_id" not in cups:
                    o_connexio = self.get_tipus_connexio(
                        cups['id_escomesa'][0]
                    )
                    search_params = [('escomesa', '=', cups['id_escomesa'][0])]
                    bloc_escomesa_id = O.GiscegisBlocsEscomeses.search(
                        search_params
                    )
                    id_escomesa = cups.get("id_escomesa")
                    o_nom_node = ''
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
                        if bloc_escomesa['vertex']:
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

                o_potencia = ''
                o_cnae = ''
                o_pot_ads = cups.get('potencia_adscrita', '0,000') or '0,000'
                o_cod_tfa = ''
                o_estat_contracte = 0
                # energies consumides
                o_anual_activa = format_f(
                    cups['cne_anual_activa'] or 0.0, decimals=3)
                o_anual_reactiva = format_f(
                    cups['cne_anual_reactiva'] or 0.0, decimals=3)

                if polissa_id:
                    fields_to_read = [
                        'potencia', 'cnae', 'tarifa', 'butlletins', 'tensio'
                    ]

                    polissa = O.GiscedataPolissa.read(
                        polissa_id[0], fields_to_read, context_glob
                    )
                    if polissa['tensio']:
                        o_tensio = format_f(
                            float(polissa['tensio']) / 1000.0, decimals=3)
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
                                polissa_id[0], fields_to_read
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

                        if re.findall(CINI_TG_REGEXP, comptador['cini']):
                            o_equip = 'SMT'
                        elif comptador.get('tg', False):
                            o_equip = 'SMT'
                        else:
                            o_equip = 'MEC'
                    else:
                        o_equip = ''
                        if self.raven:
                            self.raven.captureMessage(
                                "Missing Comptador on Polissa with ID {}".format(
                                    polissa['id']
                                ),
                                level=logging.WARNING
                            )

                    if polissa['tarifa']:
                        o_cod_tfa = self.get_codi_tarifa(polissa['tarifa'][1])
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
                        if modcon['tensio']:
                            o_tensio = format_f(
                                float(modcon['tensio']) / 1000.0, decimals=3)
                        if modcon['potencia']:
                            o_potencia = format_f(
                                float(modcon['potencia']), decimals=3)
                        # Si no trobem polissa activa haurem de comprovar
                        # si permet posar CNA o no
                        if self.allow_cna:
                            o_equip = 'CNA'
                        elif modcon['polissa_id'] and modcon['data_final']:
                            meter_id = O.GiscedataPolissa.get_comptador_data(
                                [modcon['polissa_id'][0]], modcon['data_final']
                            )
                            if meter_id:
                                comptador = O.GiscedataLecturesComptador.read(
                                    meter_id, ['cini', 'tg']
                                )
                                if not comptador['cini']:
                                    comptador['cini'] = ''

                                if re.findall(CINI_TG_REGEXP,
                                              comptador['cini']):
                                    o_equip = 'SMT'
                                elif comptador.get('tg', False):
                                    o_equip = 'SMT'
                                else:
                                    o_equip = 'MEC'
                            else:
                                o_equip = ''
                                self.raven.captureMessage(
                                    "Missing Comptador on Polissa with "
                                    "ID {}".format(
                                        modcon['polissa_id'][0]
                                    ),
                                    level=logging.WARNING
                                )
                        else:
                            o_equip = ''
                            self.raven.captureMessage(
                                "ModCon with ID {} have missing Polissa or "
                                "Data Final".format(
                                    modcon_id
                                ),
                                level=logging.WARNING
                            )
                    else:
                        # No existeix modificació contractual per el CUPS
                        o_potencia = cups['potencia_conveni']
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
                                o_tensio = format_f(
                                    float(tensio_gis) / 1000.0, decimals=3)
                        else:
                            o_tensio = ''
                        if self.default_o_cnae:
                            o_cnae = self.default_o_cnae
                        if self.default_o_cod_tfa:
                            o_cod_tfa = self.default_o_cod_tfa
                        o_equip = ''

                o_any_incorporacio = self.year
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(
                        get_srid(O), (vertex['x'], vertex['y'])
                    )

                self.output_q.put([
                    o_nom_node,     # Nudo
                    format_f(res_srid[0], decimals=3),  # X
                    format_f(res_srid[1], decimals=3),  # Y
                    '',                 # Z
                    o_cnae,             # CNAE
                    o_equip,            # Equipo de medida
                    o_cod_tfa,          # Codigo de tarifa
                    o_zona,             # Zona de calidad
                    o_name,             # CUPS
                    o_codi_r1,          # Codigo de la distribuidora
                    o_codi_ine_mun,     # Municipio
                    o_codi_ine_prov,    # Provincia
                    o_connexio,         # Conexion
                    o_tensio,           # Tension de alimentacion
                    o_estat_contracte,  # Estado de contrato
                    format_f(o_potencia or '0,000', decimals=3),    # Potencia contratada
                    format_f(o_potencia_facturada, decimals=3),     # Potencia facturada
                    format_f(o_pot_ads, decimals=3),        # Potencia adscrita a la instalacion
                    format_f(o_anual_activa, decimals=3),   # Energia activa anual consumida
                    format_f(o_anual_reactiva, decimals=3), # Energia reactiva anual consumida
                    o_any_incorporacio  # Año informacion
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
