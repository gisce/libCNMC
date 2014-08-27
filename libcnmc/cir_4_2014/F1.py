# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import traceback

import click
from libcnmc.utils import N_PROC, CODIS_TARIFA, CODIS_ZONA
from libcnmc.core import MultiprocessBased


class F1(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F1, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        manager = Manager()
        self.cts = manager.dict()
        self.base_object = 'CUPS'
        self.report_name = 'F1 - CUPS'

    def get_codi_tarifa(self, codi_tarifa):
        return CODIS_TARIFA.get(codi_tarifa, '')

    def get_sequence(self):
        search_params = []
        return self.connection.GiscedataCupsPs.search(search_params)

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
                    zona_desc = dades_ct['zona_id'][1].upper().replace(' ', '')
                    if zona_desc in CODIS_ZONA:
                        zona_qualitat = CODIS_ZONA[zona_desc]
                        self.cts[codi_ct] = zona_qualitat
        return zona_qualitat

    def get_tipus_connexio(self, id_escomesa):
        O = self.connection
        bloc = O.GiscegisBlocsEscomeses.search([('escomesa', '=', id_escomesa)])
        tipus = ''
        if bloc:
            bloc = O.GiscegisBlocsEscomeses.read(bloc[0], ['node'])
            if bloc['node']:
                node = bloc['node'][0]
                edge = O.GiscegisEdge.search(['|',
                    ('start_node', '=', node),
                    ('end_node', '=', node)
                ])
                if edge:
                    edge = O.GiscegisEdge.read(edge[0], ['id_linktemplate'])
                    if edge['id_linktemplate']:
                        bt = O.GiscedataBtElement.search([('id', '=', edge['id_linktemplate'])])
                        if bt:
                            bt = O.GiscedataBtElement.read(bt[0], ['tipus_linia'])
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
                fields_to_read = ['name', 'id_escomesa', 'id_municipi', 'cne_anual_activa',
                                  'cne_anual_reactiva', 'cnmc_potencia_facturada']
                if 'et' in O.GiscedataCupsPs.fields_get():
                    fields_to_read += ['et']

                cups = O.GiscedataCupsPs.read(item, fields_to_read)
                if not cups or not cups.get('name'):
                    self.input_q.task_done()
                    continue
                o_name = cups['name'][:22]
                o_codi_ine = ''
                o_codi_prov = ''
                o_zona = ''
                o_potencia_facturada = cups['cnmc_potencia_facturada']
                if 'et' in cups:
                    o_zona = self.get_zona_qualitat(cups['et'])
                if cups['id_municipi']:
                    municipi = O.ResMunicipi.read(cups['id_municipi'][0], ['ine', 'state', 'dc'])
                    ine = municipi['ine'] + municipi['dc']
                    if municipi['state']:
                        provincia = O.ResCountryState.read(municipi['state'][0], ['code'])
                        o_codi_prov = provincia['code']
                    o_codi_ine = ine[2:]

                o_utmx = ''
                o_utmy = ''
                o_utmz = ''
                o_nom_node = ''
                o_tensio = ''
                o_connexio = ''
                if cups and cups['id_escomesa']:
                    o_connexio = self.get_tipus_connexio(cups['id_escomesa'][0])
                    search_params = [('escomesa', '=', cups['id_escomesa'][0])]
                    bloc_escomesa_id = O.GiscegisBlocsEscomeses.search(search_params)
                    if bloc_escomesa_id:
                        bloc_escomesa = O.GiscegisBlocsEscomeses.read(
                                                bloc_escomesa_id[0], ['node', 'vertex'])
                        if bloc_escomesa['vertex']:
                            vertex = O.GiscegisVertex.read(bloc_escomesa['vertex'][0],
                                                           ['x', 'y'])
                            o_utmx = round(vertex['x'], 3)
                            o_utmy = round(vertex['y'], 3)
                        if bloc_escomesa['node']:
                            search_params = [('start_node', '=',
                                              bloc_escomesa['node'][0])]
                            edge_id = O.GiscegisEdge.search(search_params)
                            if not edge_id:
                                search_params = [('end_node', '=',
                                                  bloc_escomesa['node'][0])]
                                edge_id = O.GiscegisEdge.search(search_params)
                            if edge_id:
                                edge = O.GiscegisEdge.read(
                                    edge_id[0], ['name', 'id_linktemplate']
                                )
                                o_nom_node = edge['name']
                                search_params = [('name', '=', edge['id_linktemplate'])]
                                bt_id = O.GiscedataBtElement.search(search_params)
                                if bt_id:
                                    bt = O.GiscedataBtElement.read(bt_id[0],
                                                                        ['tipus_linia',
                                                                         'voltatge'])
                                    if bt['tipus_linia']:
                                        o_linia = bt['tipus_linia'][1][0]
                                    o_tensio = float(bt['voltatge']) / 1000.0

                search_params = [('cups', '=', cups['id'])] + search_glob
                polissa_id = O.GiscedataPolissa.search(search_params, 0, 0, False,
                                                       context_glob)
                o_potencia = ''
                o_cnae = ''
                o_pot_ads = ''
                o_equip = 'MEC'
                o_cod_tfa = ''
                tg_instalat = False
                o_estat_contracte = 0
                if polissa_id:
                    fields_to_read = ['potencia', 'cnae', 'tarifa']
                    if 'butlletins' in O.GiscedataPolissa.fields_get():
                        fields_to_read += ['butlletins']
                    if 'tg' in O.GiscedataLecturesComptador.fields_get():
                        tg_instalat = True
                        fields_to_read += ['comptadors']

                    polissa = O.GiscedataPolissa.read(polissa_id[0], fields_to_read,
                             context_glob)
                    o_potencia = polissa['potencia']
                    if polissa['cnae']:
                        o_cnae = polissa['cnae'][1]
                    # Mirem si té l'actualització dels butlletins
                    if polissa['butlletins']:
                        butlleti = O.GiscedataButlleti.read(polissa['butlletins'][-1],
                                                            ['pot_max_admisible'])
                        o_pot_ads = butlleti['pot_max_admisible']
                    if tg_instalat:
                        comptadors = O.GiscedataLecturesComptador.read(
                            polissa['comptadors'], ['tg', 'active'])
                        for comptador in comptadors:
                            if comptador['active'] and comptador['tg']:
                                o_equip = 'SMT'
                    if polissa['tarifa']:
                        o_cod_tfa = self.get_codi_tarifa(polissa['tarifa'][1])
                else:
                    #Si no trobem polissa activa, considerem "Contrato no activo (CNA)"
                    o_equip = 'CNA'
                    o_estat_contracte = 1


                #energies consumides
                o_anual_activa = cups['cne_anual_activa'] or 0.0
                o_anual_reactiva = cups['cne_anual_reactiva'] or 0.0
                o_any_incorporacio = self.year + 1
                self.output_q.put([
                   o_nom_node,
                   o_utmx,
                   o_utmy,
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
                   o_potencia,
                   o_potencia_facturada,
                   o_pot_ads or o_potencia,
                   o_anual_activa,
                   o_anual_reactiva,
                   o_any_incorporacio
                ])
            except Exception, e:
                traceback.print_exc()
            finally:
                self.input_q.task_done()


@click.command()
@click.option('-q', '--quiet', default=False, help="No mostrar missatges de status per stderr")
@click.option('--interactive/--no-interactive', default=True, help="Deshabilitar el mode interactiu")
@click.option('-o', '--output', help="Fitxer de sortida")
@click.option('-c', '--codi-r1', help='Codi R1 de la distribuidora')
@click.option('-y', '--year', default=(datetime.now().year - 1),
              help=u"Any per càlculs")
@click.option('-s', '--server', default='http://localhost',
              help=u'Adreça servidor ERP')
@click.option('-p', '--port', default=8069, help='Port servidor ERP', type=click.INT)
@click.option('-u', '--user', default='admin', help='Usuari servidor ERP')
@click.option('-w', '--password', default='admin',
              help='Contrasenya usuari ERP')
@click.option('-d', '--database', help='Nom de la base de dades')
@click.option('--num-proc', default=N_PROC, type=click.INT)
def main(**kwargs):
    from ooop import OOOP
    O = OOOP(dbname=kwargs['database'], user=kwargs['user'],
             pwd=kwargs['password'], port=kwargs['port'],
             uri=kwargs['server'])
    f1 = F1(
        quiet=kwargs['quiet'],
        interactive=kwargs['interactive'],
        output=kwargs['output'],
        connection=O,
        num_proc=kwargs['num_proc'],
        codi_r1=kwargs['codi_r1'],
        year=kwargs['year']
    )
    f1.calc()






