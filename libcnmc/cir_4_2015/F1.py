# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import re
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP
from libcnmc.utils import get_ine, get_comptador, format_f, get_srid,\
    convert_srid
from libcnmc.core import MultiprocessBased
from ast import literal_eval


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
        mod_all_year = self.connection.GiscedataPolissaModcontractual.search([
            ("data_inici", "<=", "{}-01-01".format(self.year)),
            ("data_final", ">=", "{}-12-31".format(self.year))],
            0, 0, False,{"active_test": False}
        )
        mods_ini = self.connection.GiscedataPolissaModcontractual.search(
            [("data_inici", ">=", "{}-01-01".format(self.year)),
            ("data_inici", "<=", "{}-12-31".format(self.year))],
            0, 0, False,{"active_test": False}
        )
        mods_fi = self.connection.GiscedataPolissaModcontractual.search(
            [("data_final", ">=", "{}-01-01".format(self.year)),
            ("data_final", "<=", "{}-12-31".format(self.year))],
            0,0,False,{"active_test": False}
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
        :return: INE code
        :rtype:str
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

    def get_sequence(self):
        """
        Generates the list of cups to generate the F1

        :return: List of CUPS
        :rtype: list[int]
        """
        data_ini = '%s-01-01' % (self.year + 1)
        search_params = [('active', '=', True),
                         '|',
                         ('create_date', '<', data_ini),
                         ('create_date', '=', False)]
        return self.connection.GiscedataCupsPs.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_zona_qualitat(self, codi_ct):
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

    def get_tipus_connexio(self, id_escomesa):
        """
        Gets the tipus of conexio of an escomesa

        :param id_escomesa: Id of the escomesa
        :type id_escomesa: int
        :return: A or S depending on if the linia is aerial o underground
        :rtype: str
        """
        O = self.connection
        bloc = O.GiscegisBlocsEscomeses.search(
            [('escomesa', '=', id_escomesa)]
        )
        tipus = ''
        if bloc:
            bloc = O.GiscegisBlocsEscomeses.read(bloc[0], ['node'])
            if bloc['node']:
                node = bloc['node'][0]
                edge = O.GiscegisEdge.search(
                    ['|', ('start_node', '=', node), ('end_node', '=', node)]
                )
                if edge:
                    edge = O.GiscegisEdge.read(edge[0], ['id_linktemplate'])
                    if edge['id_linktemplate']:
                        bt = O.GiscedataBtElement.search(
                            [('name', '=', edge['id_linktemplate'])]
                        )
                        if bt:
                            bt = O.GiscedataBtElement.read(
                                bt[0], ['tipus_linia']
                            )
                            if bt['tipus_linia']:
                                if bt['tipus_linia'][0] == 1:
                                    tipus = 'A'
                                else:
                                    tipus = 'S'
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
            ('state', 'not in', ('esborrany', 'validar')),
            ('data_alta', '<=', ultim_dia_any),
            '|',
            ('data_baixa', '>=', ultim_dia_any),
            ('data_baixa', '=', False)
        ]
        context_glob = {'date': ultim_dia_any, 'active_test': False}
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                fields_to_read = [
                    'name', 'id_escomesa', 'id_municipi', 'cne_anual_activa',
                    'cne_anual_reactiva', 'cnmc_potencia_facturada', 'et',
                    'polisses', 'potencia_conveni', 'potencia_adscrita'
                ]
                cups = O.GiscedataCupsPs.read(item, fields_to_read)
                if not cups or not cups.get('name'):
                    self.input_q.task_done()
                    continue
                if not set(cups["polisses"]).intersection(self.modcons_in_year):
                    continue
                o_name = cups['name'][:22]
                o_codi_ine_mun = ''
                o_codi_ine_prov = ''
                o_zona = ''
                o_potencia_facturada = format_f(
                    cups['cnmc_potencia_facturada'], 3) or ''
                if 'et' in cups:
                    o_zona = self.get_zona_qualitat(cups['et'])
                if cups['id_municipi']:
                    id_mun = cups["id_municipi"][0]
                    o_codi_ine_prov, o_codi_ine_mun = self.get_ine(id_mun)
                o_utmz = ''
                o_nom_node = ''
                o_tensio = ''
                o_connexio = ''
                vertex = False
                if cups and cups['id_escomesa']:
                    o_connexio = self.get_tipus_connexio(
                        cups['id_escomesa'][0]
                    )
                    search_params = [('escomesa', '=', cups['id_escomesa'][0])]
                    bloc_escomesa_id = O.GiscegisBlocsEscomeses.search(
                        search_params
                    )
                    if bloc_escomesa_id:
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
                o_pot_ads = cups['potencia_adscrita'] or '0,000'
                o_equip = 'MEC'
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
                    if 'RE' in polissa['tarifa'][1]:
                        continue
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
                    comptador_actiu = get_comptador(
                        self.connection, polissa['id'], self.year)
                    if comptador_actiu:
                        comptador_actiu = comptador_actiu[0]
                        comptador = O.GiscedataLecturesComptador.read(
                            comptador_actiu, ['cini', 'tg']
                        )
                        if not comptador['cini']:
                            comptador['cini'] = ''
                        if comptador.get('tg', False):
                            o_equip = 'SMT'
                        elif re.findall(CINI_TG_REGEXP, comptador['cini']):
                            o_equip = 'SMT'
                        else:
                            o_equip = 'MEC'
                    if polissa['tarifa']:
                        o_cod_tfa = self.get_codi_tarifa(polissa['tarifa'][1])
                else:
                    # Si no trobem polissa activa, considerem
                    # "Contrato no activo (CNA)"

                    o_equip = 'CNA'
                    o_estat_contracte = 1

                    search_modcon = [
                        ('id', 'in', cups['polisses']),
                        ('data_inici', '<=', ultim_dia_any)
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
                            'potencia'
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

                o_any_incorporacio = self.year
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(
                        self.codi_r1, get_srid(O), (vertex['x'], vertex['y']))

                self.output_q.put([
                    o_nom_node,
                    format_f(res_srid[0], decimals=3),
                    format_f(res_srid[1], decimals=3),
                    o_utmz,
                    o_cnae,
                    o_equip,
                    o_cod_tfa,
                    o_zona,
                    o_name,
                    o_codi_r1,
                    o_codi_ine_mun,
                    o_codi_ine_prov,
                    o_connexio,
                    o_tensio,
                    o_estat_contracte,
                    format_f(o_potencia or '0,000', decimals=3),
                    format_f(o_potencia_facturada, decimals=3),
                    format_f(o_pot_ads, decimals=3),
                    format_f(o_anual_activa, decimals=3),
                    format_f(o_anual_reactiva, decimals=3),
                    o_any_incorporacio
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
