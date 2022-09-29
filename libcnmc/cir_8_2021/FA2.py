#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Informe de la CNMC relatiu a la generació conectada de les xarxes de distribució
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
from libcnmc.utils import parse_geom


class FA2(MultiprocessBased):

    def __init__(self, **kwargs):
        super(FA2, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB5 - TRAFOS-SE'
        self.base_object = 'TRAFOS'

    def get_municipi(self, cups):
        o = self.connection
        municipi_name = False
        municipi = o.GiscedataCupsPs.read(cups[0], ['id_municipi'])['id_municipi']
        if municipi:
            municipi_name = municipi[1]
        return municipi_name

    def get_tension(self, cups):
        o = self.connection
        tensio = False
        polissa = o.GiscedataCupsPs.read(cups[0], ['polissa_polissa'])['polissa_polissa']
        if polissa:
            polissa_id = polissa[0]
            tensio = o.GiscedataPolissa.read(polissa_id, ['tensio_normalitzada'])['tensio_normalitzada']
            if tensio:
                tensio_id = tensio[0]
                tensio = o.GiscedataTensionsTensio.read(tensio_id, ['tensio'])['tensio']
        return tensio

    def get_energies(self, cups):
        o = self.connection
        res = {
            'energia_activa_producida': False,
            'energia_activa_consumida': False,
            'energia_reactiva_producida': False,
            'energia_reactiva_consumida': False,
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
            'cau': False,
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
            'node': False,
            'x': False,
            'y': False,
        }
        escomesa_data = o.GiscedataCupsPs.read(cups[0], ['id_escomesa'])['id_escomesa']
        if escomesa_data:
            node_data = o.GiscedataCupsEscomesa.read(escomesa_data[0], ['node_id'])['node_id']
            if node_data:
                res['node'] = node_data[1]
            geom_data = o.GiscedataCupsEscomesa.read(escomesa_data[0], ['geom'])['geom']
            if geom_data:
                res['x'] = parse_geom(geom_data)[0]['x']
                res['y'] = parse_geom(geom_data)[0]['y']
        return res

    def get_serveis_aux(self, cups):
        o = self.connection
        serveis_aux = False
        cups_20 = cups[0:20]
        cups_id = o.GiscedataCupsPs.search([('name', 'ilike', cups_20), ('name', '!=', cups)])
        cups_name = o.GiscedataCupsPs.read(cups_id, ['name'])['name']
        if cups_name:
            serveis_aux = cups_name
        return serveis_aux

    def get_zona(self, cups):
        o = self.connection
        zona = False
        ct_name = o.GiscedataCupsPs.read(cups[0], ['et'])['et']

    def consumer(self):
        o = self.connection
        fields_to_read = ['provincia', 'cini', 'cups', 'potencia_nominal']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                recore = o.GiscedataRe.read(item, fields_to_read)
                cups = recore['cups']

                node_geom = self.get_node_geom(cups)
                o_nudo = node_geom['node']
                o_coordenadas_x = node_geom['x']
                o_coordenadas_y = node_geom['y']
                o_coordenadas_z = ''
                o_provincia = recore['provincia']
                o_cini = recore['cini']
                o_municipio = self.get_municipi(cups)
                o_tension = self.get_tension(cups)
                o_potencia_instalada = recore['potencia_nominal']
                energies = self.get_energies(cups)
                o_energia_activa_producida = energies['energia_activa_producida']
                o_energia_activa_consumida = energies['energia_activa_consumida']
                o_energia_reactiva_producida = energies['energia_reactiva_producida']
                o_energia_reactiva_consumida = energies['energia_reactiva_consumida']
                autoconsum = self.get_autoconsum(cups)
                o_autoconsum = autoconsum['autoconsum']
                o_cau = autoconsum['cau']
                o_cups_servicios_auxiliares = self.get_serveis_aux(cups)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()

