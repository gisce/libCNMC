#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Informe de la CNMC relatiu a la generació conectada de les xarxes de distribució
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import get_tipus_connexio, format_f, get_ine, convert_srid, get_srid, get_serveis_aux
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
        O = self.connection

        data_pm = '%s-01-01' % (self.year + 1)

        re_ids = []
        for re_id in O.GiscedataRe.search([]):
            if O.GiscedataRe.read(re_id, ['uprs']).get('uprs'):
                for upr in O.GiscedataRe.read(re_id, ['uprs'])['uprs']:
                    upr_data = O.GiscedataReUprs.read(upr, ['data_alta'])
                    if upr_data.get('data_alta'):
                        if upr_data['data_alta'] < data_pm:
                            re_ids.append(re_id)
                            break


        for elem in range(0, len(re_ids)):
            re_ids[elem] = 're.{}'.format(re_ids[elem])

        search_params_ac = [
            ('data_alta', '<', data_pm), ('collectiu', '=', True)
        ]
        autoconsum_ids = O.GiscedataAutoconsum.search(
            search_params_ac, 0, 0, False, {"active_test": False})
        search_params_gen = [('autoconsum_id', 'in', autoconsum_ids)]
        generador_ids = O.GiscedataAutoconsumGenerador.search(
            search_params_gen, 0, 0, False, {"active_test": False})

        for elem in range(0, len(generador_ids)):
             generador_ids[elem] = 'gac.{}'.format(generador_ids[elem])

        return list(set(re_ids)) + list(set(generador_ids))

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
        O = self.connection
        res = {
            'id_municipi': '',
            'id_provincia': '',
        }
        municipi_provincia_data = O.GiscedataCupsPs.read(cups[0], ['id_municipi', 'id_provincia'])
        if municipi_provincia_data.get('id_municipi', False):
            res['id_municipi'] = municipi_provincia_data['id_municipi']
        if municipi_provincia_data.get('id_provincia', False):
            res['id_provincia'] = municipi_provincia_data['id_provincia']
        return res

    def get_tension(self, cups):
        O = self.connection
        polissa_obj = O.GiscedataPolissa
        tensio = 0
        cups_20 = cups[1][0:20]
        search_params = [('cups', 'like', cups_20), ('tarifa.name', 'like', 'RE')]
        polissa = polissa_obj.search(search_params, 0, 0, 'data_alta desc', {'active_test': False})

        if polissa:
            polissa_id = polissa[0]
            tensio = O.GiscedataPolissa.read(polissa_id, ['tensio_normalitzada'])['tensio_normalitzada']
            if tensio:
                tensio_id = tensio[0]
                tensio = format_f(float(O.GiscedataTensionsTensio.read(tensio_id, ['tensio'])['tensio'])/1000, decimals=3)
        return tensio

    def get_energies(self, cups):
        O = self.connection
        res = {
            'energia_activa_producida': '0,000',
            'energia_activa_consumida': '0,000',
            'energia_reactiva_producida': '0,000',
            'energia_reactiva_consumida': '0,000',
        }
        # Energia activa producida
        energia_activa_prod_data = O.GiscedataCupsPs.read(
            cups[0], ['cne_anual_activa_generada'])
        if energia_activa_prod_data.get('cne_anual_activa_generada', False):
            res['energia_activa_producida'] = format_f(energia_activa_prod_data['cne_anual_activa_generada'], decimals=3)

        # Energia activa consumida
        energia_activa_consumida_data = O.GiscedataCupsPs.read(
            cups[0], ['cne_anual_activa'])
        if energia_activa_consumida_data.get('cne_anual_activa', False):
            res['energia_activa_consumida'] = format_f(energia_activa_consumida_data['cne_anual_activa'], decimals=3)

        # Energia reactiva producida
        energia_reactiva_prod_data = O.GiscedataCupsPs.read(
            cups[0], ['cne_anual_reactiva_generada'])
        if energia_reactiva_prod_data.get('cne_anual_reactiva_generada', False):
            res['energia_reactiva_producida'] = format_f(energia_reactiva_prod_data['cne_anual_reactiva_generada'], decimals=3)

        # Energia reactiva consumida
        energia_reactiva_cons_data = O.GiscedataCupsPs.read(
            cups[0], ['cne_anual_reactiva'])
        if energia_reactiva_cons_data.get('cne_anual_reactiva', False):
            res['energia_reactiva_consumida'] = format_f(energia_reactiva_cons_data['cne_anual_reactiva'], decimals=3)
        return res

    def get_autoconsum(self, cups):
        O = self.connection
        res = {
            'autoconsum': '0',
            'cau': '',
        }
        autoconsum_data = O.GiscedataCupsPs.read(cups[0], ['autoconsum_id'])
        if autoconsum_data.get('autoconsum_id', False):
            res['autoconsum'] = '1'
            autoconsum_id = autoconsum_data['autoconsum_id'][0]
            cau_data = O.GiscedataAutoconsum.read(autoconsum_id, ['cau'])
            if cau_data.get('cau', False):
                res['cau'] = cau_data['cau']
        return res

    def get_zona(self, cups):
        O = self.connection
        zona = ''
        ct_name = O.GiscedataCupsPs.read(cups[0], ['et'])['et']
        if ct_name:
            ct_id = O.GiscedataCts.search([('name', '=', ct_name)])
            if ct_id:
                zona_id = O.GiscedataCts.read(ct_id, ['zona_id'])[0]['zona_id']
                if zona_id:
                    zona_data = zona_id[1].upper()
                    zona = ZONA[zona_data]
        return zona

    def consumer(self):
        O = self.connection
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                model = item.split('.')[0]
                id_ = int(item.split('.')[1])

                # RECORE
                if model == 're':
                    fields_to_read = [
                        'provincia', 'cini', 'cups', 'potencia_nominal', 'cil',
                        'cups_serveis_aux_id'
                    ]
                    recore = O.GiscedataRe.read(id_, fields_to_read)
                    cups = recore['cups']

                    # Coordenades I Node
                    vertex = ''
                    o_nudo = ''

                    cups_data = O.GiscedataCupsPs.read(cups[0], ['id_escomesa'])['id_escomesa']
                    if cups_data:
                        esco_data = O.GiscedataCupsEscomesa.read(cups_data[0], ['node_id'])['node_id']
                        if esco_data:
                            o_nudo = esco_data[1]
                        geom_data = O.GiscedataCupsEscomesa.read(cups_data[0], ['geom'])
                        if geom_data.get('geom', False):
                            geom = wkt.loads(geom_data['geom']).coords[0]
                            vertex = {"x": geom[0], "y": geom[1]}

                    res_srid = ['', '']
                    if vertex:
                        res_srid = convert_srid(
                            get_srid(O), (vertex['x'], vertex['y'])
                        )

                    # CIL
                    o_cil = '{}{}'.format(cups[1], '001')

                    # CINI
                    o_cini = ''
                    if recore.get('cini', False):
                        o_cini = recore['cini']

                    # Municipi + Provincia
                    o_municipio = ''
                    o_provincia = ''
                    cups_id = O.GiscedataCupsPs.search([('name', '=', cups[1])])
                    if cups_id:
                        municipi_data = O.GiscedataCupsPs.read(cups_id, ['id_municipi'])[0]
                        if municipi_data.get('id_municipi', False):
                            municipi_id = municipi_data['id_municipi'][0]
                            o_provincia, o_municipio = self.get_ine(municipi_id)

                    # Zona
                    o_zona = self.get_zona(cups)

                    # Connexió
                    o_connexion = ''
                    id_escomesa = O.GiscedataCupsPs.read(cups[0], ['id_escomesa'])
                    if id_escomesa.get('id_escomesa', False):
                        o_connexion = get_tipus_connexio(O, id_escomesa['id_escomesa'][0])

                    # Tensió
                    o_tension = self.get_tension(cups)

                    # Potència instalada
                    o_potencia_instalada = format_f(recore['potencia_nominal'], decimals=3)

                    # Autoconsum + CAU
                    autoconsum = self.get_autoconsum(cups)
                    o_autoconsum = autoconsum['autoconsum']
                    o_cau = autoconsum['cau']

                    # Serveis auxiliars
                    o_cups_ssaa = get_serveis_aux(O, id_)

                    # Energia consumida
                    o_energia_activa_consumida = '0,000'
                    o_energia_reactiva_consumida = '0,000'
                    if o_cups_ssaa:
                        energies = self.get_energies(o_cups_ssaa)
                        o_energia_activa_consumida = energies['energia_activa_consumida']
                        o_energia_reactiva_consumida = energies['energia_reactiva_consumida']

                    # Energia produïda
                    energies = self.get_energies(cups)
                    o_energia_activa_producida = energies['energia_activa_producida']
                    o_energia_reactiva_producida = energies['energia_reactiva_producida']

                    o_cups_ssaa = o_cups_ssaa[1] if o_cups_ssaa else ''
                    self.output_q.put([
                        o_nudo,                                         # Node
                        format_f(res_srid[0], decimals=3),              # Coordenada x
                        format_f(res_srid[1], decimals=3),              # Coordenada y
                        '0,000',                                        # Coordenada z
                        o_cil,                                          # CIL
                        o_cini,                                         # CINI
                        o_municipio,                                    # Municipi
                        o_provincia,                                    # Provincia
                        o_zona,                                         # Zona
                        o_connexion,                                    # Connexió
                        ('{}'.format(o_tension)).replace('.', ','),     # Tensió
                        o_potencia_instalada,                           # Potència instalada
                        o_energia_activa_producida,                     # Energia activa produida
                        o_energia_activa_consumida,                     # Energia activa consumida
                        o_energia_reactiva_producida,                   # Energia reactiva produida
                        o_energia_reactiva_consumida,                   # Energia reactiva consumida
                        o_autoconsum,                                   # Autoconsum
                        o_cau,                                          # CAU
                        o_cups_ssaa                                     # Serveis auxiliars
                    ])

                # GENERADOR AUTOCONSUM
                elif model == 'gac':
                    fields_to_read = [
                        'id_escomesa', 'utm_x', 'utm_y', 'cil', 'cini',
                        'pot_instalada_gen', 'autoconsum_id',
                        'cne_anual_activa', 'cne_anual_activa_generada',
                        'cne_anual_reactiva', 'cne_anual_reactiva_generada'

                    ]
                    gen = O.GiscedataAutoconsumGenerador.read(
                        id_, fields_to_read)

                    # Coordenades i Node
                    o_nudo = ''
                    node_id = False
                    esco_id = False
                    tensio = 0.0
                    vertex = False
                    if gen.get('id_escomesa'):
                        esco_id = gen['id_escomesa'][0]
                        if esco_id:
                            esco_data = O.GiscedataCupsEscomesa.read(
                                esco_id, ['node_id', 'tensio', 'geom'])
                            if esco_data.get('node_id'):
                                node = esco_data['node_id']
                                node_id = node[0]
                                o_nudo = node[1]
                            # Preparar per camp TENSION
                            if esco_data.get('tensio'):
                                tensio = float(esco_data['tensio'])
                            if esco_data.get('geom'):
                                geom = wkt.loads(esco_data['geom']).coords[0]
                                vertex = {"x": geom[0], "y": geom[1]}

                    res_srid = ['', '']
                    if vertex:
                        res_srid = convert_srid(
                            get_srid(O), (vertex['x'], vertex['y'])
                        )

                    # CIL
                    o_cil = gen['cil'] if gen.get('cil') else ''

                    # CINI
                    o_cini = gen['cini'] if gen.get('cini') else ''

                    # Municipi + Provincia
                    o_municipio = ''
                    o_provincia = ''
                    if node_id:
                        node_data = O.GiscegisNodes.read(
                            node_id, ['municipi_id'])
                        if node_data.get('municipi_id'):
                            municipi_id = node_data['municipi_id'][0]
                            o_provincia, o_municipio = self.get_ine(municipi_id)

                    # Zona
                    o_zona = ''
                    cau = ''
                    if gen.get('autoconsum_id'):
                        ac_id = gen['autoconsum_id'][0]
                        ac_data = O.GiscedataAutoconsum.read(
                            ac_id, ['cups_id', 'cau'])
                        if ac_data.get('cups_id'):
                            cups = ac_data['cups_id']
                            o_zona = self.get_zona(cups)
                        # Preparar per camp CAU
                        if ac_data.get('cau'):
                            cau = ac_data['cau']

                    # Connexió
                    o_connexion = (
                        get_tipus_connexio(O, esco_id) if esco_id else '')

                    # Tensió
                    o_tension = tensio

                    # Potència instalada
                    o_potencia_instalada = ''
                    if gen.get('pot_instalada_gen'):
                        o_potencia_instalada = gen['pot_instalada_gen']

                    # Autoconsum
                    o_autoconsum = 1 # Es força a valor fixe 1 (Autoconsum)

                    # CAU
                    o_cau = cau

                    # Serveis auxiliars
                    o_cups_ssaa = '' # Es força a valor fixe buit

                    # Energies
                    o_energia_activa_producida = ''
                    o_energia_activa_consumida = ''
                    o_energia_reactiva_producida = ''
                    o_energia_reactiva_consumida = ''

                    if gen.get('cne_anual_activa_generada'):
                        o_energia_activa_producida = gen[
                            'cne_anual_activa_generada']
                    if gen.get('cne_anual_activa'):
                        o_energia_activa_consumida = gen['cne_anual_activa']
                    if gen.get('cne_anual_reactiva_generada'):
                        o_energia_reactiva_producida = gen[
                            'cne_anual_reactiva_generada']
                    if gen.get('cne_anual_reactiva'):
                        o_energia_reactiva_consumida = gen['cne_anual_reactiva']

                    self.output_q.put([
                        o_nudo,                                         # Node
                        format_f(res_srid[0], decimals=3),              # Coordenada x
                        format_f(res_srid[1], decimals=3),              # Coordenada y
                        '0,000',                                        # Coordenada z
                        o_cil,                                          # CIL
                        o_cini,                                         # CINI
                        o_municipio,                                    # Municipi
                        o_provincia,                                    # Provincia
                        o_zona,                                         # Zona
                        o_connexion,                                    # Connexió
                        format_f(o_tension / 1000.0, decimals=3),           # Tensió
                        format_f(o_potencia_instalada, decimals=3),         # Potència instalada
                        format_f(o_energia_activa_producida, decimals=3),   # Energia activa produida
                        format_f(o_energia_activa_consumida, decimals=3),   # Energia activa consumida
                        format_f(o_energia_reactiva_producida, decimals=3), # Energia reactiva produida
                        format_f(o_energia_reactiva_consumida, decimals=3), # Energia reactiva consumida
                        o_autoconsum,                                   # Autoconsum
                        o_cau,                                          # CAU
                        o_cups_ssaa                                     # Serveis auxiliars
                    ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()

