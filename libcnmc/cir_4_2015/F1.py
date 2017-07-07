# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import re
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP
from libcnmc.utils import get_ine, get_comptador, format_f, get_srid,\
    convert_srid
from libcnmc.core import MultiprocessBased


class F1(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F1, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        manager = Manager()
        self.cts = manager.dict()
        self.cnaes = manager.dict()
        self.base_object = 'CUPS'
        self.report_name = 'F1 - CUPS'

    def get_codi_tarifa(self, codi_tarifa):
        return CODIS_TARIFA.get(codi_tarifa, '')

    def get_sequence(self):
        data_ini = '%s-01-01' % (self.year + 1)
        search_params = [('active', '=', True),
                         '|',
                         ('create_date', '<', data_ini),
                         ('create_date', '=', False)]
        return self.connection.GiscedataCupsPs.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_zona_qualitat(self, codi_ct):
        zona_qualitat = ''
        if codi_ct:
            if codi_ct in self.cts:
                return self.cts[codi_ct]
            else:
                ct_ids = self.connection.GiscedataCts.search(
                    [('name', '=', codi_ct)])
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
                    'polisses', 'potencia_conveni'
                ]

                cups = O.GiscedataCupsPs.read(item, fields_to_read)
                if not cups or not cups.get('name'):
                    self.input_q.task_done()
                    continue
                o_name = cups['name'][:22]
                o_codi_ine = ''
                o_codi_prov = ''
                o_zona = ''
                o_potencia_facturada = format_f(
                    cups['cnmc_potencia_facturada'], 3) or ''
                if 'et' in cups:
                    o_zona = self.get_zona_qualitat(cups['et'])
                if cups['id_municipi']:
                    municipi = O.ResMunicipi.read(
                        cups['id_municipi'][0], ['ine']
                    )
                    ine = get_ine(self.connection, municipi['ine'])
                    o_codi_ine = ine[1]
                    o_codi_prov = ine[0]

                o_utmx = ''
                o_utmy = ''
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
                            o_utmx = round(vertex['x'], 3)
                            o_utmy = round(vertex['y'], 3)
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
                o_pot_ads = ''
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
                    if polissa['tensio']:
                        o_tensio = format_f(
                            float(polissa['tensio']) / 1000.0, decimals=3)
                    o_potencia = format_f(
                        polissa['potencia'], decimals=3)
                    if polissa['cnae']:
                        cnae_id = polissa['cnae'][0]
                        if cnae_id in self.cnaes:
                            o_cnae = self.cnaes[cnae_id]
                        else:
                            o_cnae = O.GiscemiscCnae.read(
                                cnae_id, ['name']
                            )['name']
                            self.cnaes[cnae_id] = o_cnae
                    # Mirem si té l'actualització dels butlletins
                    if polissa.get('butlletins', []):
                        butlleti = O.GiscedataButlleti.read(
                            polissa['butlletins'][-1], ['pot_max_admisible']
                        )
                        o_pot_ads = format_f(
                            butlleti['pot_max_admisible'], decimals=3)
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

                o_any_incorporacio = self.year
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(
                        self.codi_r1, get_srid(O), [vertex['x'], vertex['y']])

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
                    o_codi_ine,
                    o_codi_prov,
                    o_connexio,
                    o_tensio,
                    o_estat_contracte,
                    format_f(o_potencia or '0,000', decimals=3),
                    format_f(o_potencia_facturada, decimals=3),
                    format_f(o_pot_ads or o_potencia or '0,000', decimals=3),
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
