#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Informe de la CNMC relatiu a la generació conectada de les xarxes de distribució
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
from libcnmc.utils import parse_geom, get_ines, get_tipus_connexio

ZONA = {
    'RURAL CONCENTRADA': 'RC',
    'RURAL DISPERSA': 'RD',
    'URBANA': 'U',
    'SEMIURBANA': 'SU'
}


class FA2(MultiprocessBased):

    def __init__(self, **kwargs):
        super(FA2, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'Formulario A2: Información relativa a la generación conectada a sus redes de distribución'
        self.base_object = 'RE'

    def get_sequence(self):
        return self.connection.GiscedataRe.search([('active', '=', True)])

    def get_municipi_provincia(self, cups):
        o = self.connection
        res = {
            'id_municipi': '',
            'id_provincia': '',
        }
        municipi_provincia_data = o.GiscedataCupsPs.read(cups[0], ['id_municipi', 'id_provincia'])
        if municipi_provincia_data.get('id_municipi', False):
            res['id_municipi'] = municipi_provincia_data['id_municipi']
        if municipi_provincia_data.get('id_provincia', False):
            res['id_provincia'] = municipi_provincia_data['id_provincia']
        return res

    def get_tension(self, cups):
        o = self.connection
        tensio = ''
        polissa = o.GiscedataCupsPs.read(cups[0], ['polissa_polissa'])['polissa_polissa']
        if polissa:
            polissa_id = polissa[0]
            tensio = o.GiscedataPolissa.read(polissa_id, ['tensio_normalitzada'])['tensio_normalitzada']
            if tensio:
                tensio_id = tensio[0]
                tensio = '{:.3f}'.format(o.GiscedataTensionsTensio.read(tensio_id, ['tensio'])['tensio'])
        return tensio

    def get_energies(self, cups):
        o = self.connection
        res = {
            'energia_activa_producida': '',
            'energia_activa_consumida': '',
            'energia_reactiva_producida': '',
            'energia_reactiva_consumida': '',
        }
        # Energia activa producida
        energia_activa_prod_data = o.GiscedataCupsPs.read(
            cups[0], ['cne_anual_activa_generada'])['cne_anual_activa_generada']
        if energia_activa_prod_data:
            res['energia_activa_producida'] = energia_activa_prod_data

        # Energia activa consumida
        energia_activa_consumida_data = o.GiscedataCupsPs.read(
            cups[0], ['cne_anual_activa'])['cne_anual_activa']
        if energia_activa_consumida_data:
            res['energia_activa_consumida'] = energia_activa_consumida_data

        # Energia reactiva producida
        energia_reactiva_prod_data = o.GiscedataCupsPs.read(
            cups[0], ['cne_anual_reactiva_generada'])['cne_anual_reactiva_generada']
        if energia_reactiva_prod_data:
            res['energia_reactiva_producida'] = energia_reactiva_prod_data

        # Energia reactiva consumida
        energia_reactiva_cons_data = o.GiscedataCupsPs.read(
            cups[0], ['cne_anual_reactiva'])['cne_anual_reactiva']
        if energia_reactiva_cons_data:
            res['energia_reactiva_consumida'] = energia_reactiva_cons_data
        return res

    def get_autoconsum(self, cups):
        o = self.connection
        res = {
            'autoconsum': '0',
            'cau': '',
        }
        autoconsum_data = o.GiscedataCupsPs.read(cups[0], ['autoconsum_id'])['autoconsum_id']
        if autoconsum_data:
            res['autoconsum'] = '1'
            cau_data = o.GiscedataAutoconsum.read(autoconsum_data[0], ['cau'])
            if cau_data:
                res['cau'] = cau_data[1]
        return res

    def get_node_geom(self, cups):
        o = self.connection
        res = {
            'node': '',
            'x': '',
            'y': '',
        }
        escomesa_data = o.GiscedataCupsPs.read(cups[0], ['id_escomesa'])['id_escomesa']
        if escomesa_data:
            node_data = o.GiscedataCupsEscomesa.read(escomesa_data[0], ['node_id'])['node_id']
            if node_data:
                res['node'] = node_data[1]
            geom_data = o.GiscedataCupsEscomesa.read(escomesa_data[0], ['geom'])['geom']
            if geom_data:
                res['x'] = '{:.3f}'.format(parse_geom(geom_data)[0]['x'])
                res['y'] = '{:.3f}'.format(parse_geom(geom_data)[0]['y'])
        return res

    def get_serveis_aux(self, cups):
        o = self.connection
        serveis_aux = ''
        cups_20 = cups[0:20]
        cups_id = o.GiscedataCupsPs.search([('name', 'ilike', cups_20), ('name', '!=', cups)])
        cups_name = o.GiscedataCupsPs.read(cups_id, ['name'])['name']
        if cups_name:
            serveis_aux = cups_name
        return serveis_aux

    def get_zona(self, cups):
        o = self.connection
        zona = ''
        ct_name = o.GiscedataCupsPs.read(cups[0], ['et'])['et']
        if ct_name:
            ct_id = o.GiscedataCts.search([('name', '=', ct_name)])
            if ct_id:
                zona_id = o.GiscedataCts.read(ct_id, ['zona_id'])['zona_id']
                if zona_id:
                    zona_data = zona_id[1].upper()
                    zona = ZONA[zona_data]
        return zona

    def consumer(self):
        o = self.connection
        fields_to_read = ['provincia', 'cini', 'cups', 'potencia_nominal']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                recore = o.GiscedataRe.read(item, fields_to_read)
                cups = recore['cups']

                # Node
                node_geom = self.get_node_geom(cups)
                o_nudo = node_geom['node']

                # Coordenades
                o_coordenadas_x = node_geom['x']
                o_coordenadas_y = node_geom['y']
                o_coordenadas_z = ''

                # CINI
                o_cini = recore['cini']

                # Municipi + Provincia
                municipi_provincia_data = self.get_municipi_provincia(o, cups)
                o_municipio = get_ines(municipi_provincia_data)['ine_municipi']
                o_provincia = get_ines(municipi_provincia_data)['ine_provincia']

                # Zona
                o_zona = self.get_zona(cups)

                # Connexió
                id_escomesa = o.GiscedataCupsPs.read(cups[0], ['id_escomesa'])['id_escomesa']
                o_connexion = get_tipus_connexio(o, id_escomesa)

                # Tensió
                o_tension = self.get_tension(cups)

                # Potència instalada
                o_potencia_instalada = '{:.3f}'.format(recore['potencia_nominal'])

                # Energia produïda/consumida
                energies = self.get_energies(cups)
                o_energia_activa_producida = '{:.3f}'.format(energies['energia_activa_producida'])
                o_energia_activa_consumida = '{:.3f}'.format(energies['energia_activa_consumida'])
                o_energia_reactiva_producida = '{:.3f}'.format(energies['energia_reactiva_producida'])
                o_energia_reactiva_consumida = '{:.3f}'.format(energies['energia_reactiva_consumida'])

                # Autoconsum + CAU
                autoconsum = self.get_autoconsum(cups)
                o_autoconsum = autoconsum['autoconsum']
                o_cau = autoconsum['cau']

                # Serveis auxiliars
                o_cups_servicios_auxiliares = self.get_serveis_aux(cups)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
