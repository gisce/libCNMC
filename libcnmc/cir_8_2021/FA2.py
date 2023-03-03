#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Informe de la CNMC relatiu a la generació conectada de les xarxes de distribució
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import parse_geom, get_tipus_connexio, format_f, get_ine, convert_srid, get_srid
from shapely import wkt

ZONA = {
    'RURAL CONCENTRADA': 'RC',
    'RURAL DISPERSA': 'RD',
    'URBANA': 'U',
    'SEMIURBANA': 'SU'
}


class FA2(StopMultiprocessBased):

    def __init__(self, **kwargs):
        super(FA2, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'Formulario A2: Información relativa a la generación conectada a sus redes de distribución'
        self.base_object = 'RE'

    def get_sequence(self):
        re_ids = self.connection.GiscedataRe.search([('active', '=', True)])
        for elem in range(0, len(re_ids)):
            re_ids[elem] = 're.{}'.format(re_ids[elem])

        # autoconsum_ids = self.connection.GiscedataAutoconsum.search([('subseccio', '=', )])
        # for elem in range(0, len(autoconsum_ids)):
        #     autoconsum_ids[elem] = 'au.{}'.format(autoconsum_ids[elem])
        print(re_ids)
        return re_ids

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
                tensio = format_f(float(o.GiscedataTensionsTensio.read(tensio_id, ['tensio'])['tensio'])/1000, decimals=3)
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
            cups[0], ['cne_anual_activa_generada'])
        if energia_activa_prod_data.get('cne_anual_activa_generada', False):
            res['energia_activa_producida'] = format_f(energia_activa_prod_data['cne_anual_activa_generada'], decimals=3)

        # Energia activa consumida
        energia_activa_consumida_data = o.GiscedataCupsPs.read(
            cups[0], ['cne_anual_activa'])
        if energia_activa_consumida_data.get('cne_anual_activa', False):
            res['energia_activa_consumida'] = format_f(energia_activa_consumida_data['cne_anual_activa'], decimals=3)

        # Energia reactiva producida
        energia_reactiva_prod_data = o.GiscedataCupsPs.read(
            cups[0], ['cne_anual_reactiva_generada'])
        if energia_reactiva_prod_data.get('cne_anual_reactiva_generada', False):
            res['energia_reactiva_producida'] = format_f(energia_reactiva_prod_data['cne_anual_reactiva_generada'], decimals=3)

        # Energia reactiva consumida
        energia_reactiva_cons_data = o.GiscedataCupsPs.read(
            cups[0], ['cne_anual_reactiva'])
        if energia_reactiva_cons_data.get('cne_anual_reactiva', False):
            res['energia_reactiva_consumida'] = format_f(energia_reactiva_cons_data['cne_anual_reactiva'], decimals=3)
        return res

    def get_autoconsum(self, cups):
        o = self.connection
        res = {
            'autoconsum': '0',
            'cau': '',
        }
        autoconsum_data = o.GiscedataCupsPs.read(cups[0], ['autoconsum_id'])
        if autoconsum_data.get('autoconsum_id', False):
            res['autoconsum'] = '1'
            autoconsum_id = autoconsum_data['autoconsum_id']
            cau_data = o.GiscedataAutoconsum.read(autoconsum_id, ['cau'])
            if cau_data.get('cau', False):
                res['cau'] = cau_data['cau']
        return res

    def get_serveis_aux(self, cups, item):
        o = self.connection
        serveis_aux = ''

        cups_20 = cups[1][0:20]
        cups_id = o.GiscedataCupsPs.search([('name', 'ilike', cups_20), ('name', '!=', cups)])
        if cups_id:
            cups_name = o.GiscedataCupsPs.read(cups_id, ['name'])
            if cups_name:
                if cups_name[0].get('name', False):
                    serveis_aux = cups_name[0]['name']
        else:
            polissa_obj = o.GiscedataPolissa
            polissa_id = polissa_obj.search([('re_installation_id', '=', item)])
            if polissa_id:
                polissa_data = polissa_obj.read(polissa_id[0], ['cups'])
                if polissa_data.get('cups', False):
                    serveis_aux = polissa_data['cups'][1]

        return serveis_aux

    def get_zona(self, cups):
        o = self.connection
        zona = ''
        ct_name = o.GiscedataCupsPs.read(cups[0], ['et'])['et']
        if ct_name:
            ct_id = o.GiscedataCts.search([('name', '=', ct_name)])
            if ct_id:
                zona_id = o.GiscedataCts.read(ct_id, ['zona_id'])[0]['zona_id']
                if zona_id:
                    zona_data = zona_id[1].upper()
                    zona = ZONA[zona_data]
        return zona

    def consumer(self):
        o = self.connection
        fields_to_read = ['provincia', 'cini', 'cups', 'potencia_nominal', 'cil']
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                model = item.split('.')[0]
                id_ = int(item.split('.')[1])
                if model == 're':
                    recore = o.GiscedataRe.read(id_, fields_to_read)
                    cups = recore['cups']

                # Coordenades I Node
                vertex = ''
                o_nudo = ''
                o_coordenadas_z = ''
                res_srid = ['', '']

                escomesa_data = o.GiscedataCupsPs.read(cups[0], ['id_escomesa'])['id_escomesa']
                if escomesa_data:
                    node_data = o.GiscedataCupsEscomesa.read(escomesa_data[0], ['node_id'])['node_id']
                    if node_data:
                        o_nudo = node_data[1]
                    geom_data = o.GiscedataCupsEscomesa.read(escomesa_data[0], ['geom'])
                    if geom_data.get('geom', False):
                        geom = wkt.loads(geom_data['geom']).coords[0]
                        vertex = {"x": geom[0], "y": geom[1]}

                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(
                        get_srid(o), (vertex['x'], vertex['y'])
                    )

                # CIL
                o_cil = ''
                if recore.get('cil', False):
                    o_cil = recore['cil'][1]

                # CINI
                o_cini = ''
                if recore.get('cini', False):
                    o_cini = recore['cini']

                # Municipi + Provincia
                o_municipio = ''
                o_provincia = ''
                cups_id = o.GiscedataCupsPs.search([('name', '=', cups[1])])
                if cups_id:
                    municipi_data = o.GiscedataCupsPs.read(cups_id, ['id_municipi'])[0]
                    if municipi_data.get('id_municipi', False):
                        municipi_id = municipi_data['id_municipi'][0]
                        o_provincia, o_municipio = self.get_ine(municipi_id)

                # Zona
                o_zona = self.get_zona(cups)

                # Connexió
                o_connexion = ''
                id_escomesa = o.GiscedataCupsPs.read(cups[0], ['id_escomesa'])
                if id_escomesa.get('id_escomesa', False):
                    o_connexion = get_tipus_connexio(o, id_escomesa['id_escomesa'][0])

                # Tensió
                o_tension = self.get_tension(cups)

                # Potència instalada
                o_potencia_instalada = format_f(recore['potencia_nominal'], decimals=3)

                # Energia produïda/consumida
                energies = self.get_energies(cups)
                o_energia_activa_producida = energies['energia_activa_producida']
                o_energia_activa_consumida = energies['energia_activa_consumida']
                o_energia_reactiva_producida = energies['energia_reactiva_producida']
                o_energia_reactiva_consumida = energies['energia_reactiva_consumida']

                # Autoconsum + CAU
                autoconsum = self.get_autoconsum(cups)
                o_autoconsum = autoconsum['autoconsum']
                o_cau = autoconsum['cau']

                # Serveis auxiliars
                o_cups_servicios_auxiliares = self.get_serveis_aux(cups, item)

                self.output_q.put([
                    o_nudo,  # Node
                    format_f(res_srid[0], decimals=3),  # Coordenada x
                    format_f(res_srid[1], decimals=3),  # Coordenada y
                    o_coordenadas_z,  # Coordenada z
                    o_cil,  # CIL
                    o_cini,  # CINI
                    o_municipio,  # Municipi
                    o_provincia,  # Provincia
                    o_zona,  # Zona
                    o_connexion,  # Connexió
                    ('{}'.format(o_tension)).replace('.', ','),  # Tensió
                    o_potencia_instalada,  # Potència instalada
                    o_energia_activa_producida,  # Energia activa produida
                    o_energia_activa_consumida,  # Energia activa consumida
                    o_energia_reactiva_producida,  # Energia reactiva produida
                    o_energia_reactiva_consumida,  # Energia reactiva consumida
                    o_autoconsum,  # Autoconsum
                    o_cau,  # CAU
                    o_cups_servicios_auxiliares  # Serveis auxiliars
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()

