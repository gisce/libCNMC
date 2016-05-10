#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
from datetime import datetime
import traceback
import math
import time

from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f, tallar_text


class LAT(MultiprocessBased):
    def __init__(self, **kwargs):
        super(LAT, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies AT'
        self.report_name = 'CNMC INVENTARI AT'
        self.layer = 'LBT\_%'
        self.embarrats = kwargs.pop('embarrats', False)
        id_res_like = self.connection.ResConfig.search(
            [('name', '=', 'giscegis_btlike_layer')])
        if id_res_like:
            self.layer = self.connection.ResConfig.read(
                id_res_like, ['value'])[0]['value']

    def get_sequence(self):

        search_params = [('propietari', '=', True)]
        obj_lat = self.connection.GiscedataAtLinia
        ids = obj_lat.search(search_params)
        id_lat_emb = []
        if self.embarrats:
            id_lat_emb = obj_lat.search(
                [('name', '=', '1')], 0, 0, False, {'active_test': False})
        return ids + id_lat_emb

    def consumer(self):
        O = self.connection
        fields_to_read = ['baixa', 'data_pm', 'data_industria',
                          'coeficient', 'cini', 'propietari',
                          'tensio_max_disseny', 'name', 'origen',
                          'final', 'perc_financament', 'circuits',
                          'longitud_cad', 'cable', 'cnmc_tipo_instalacion',
                          'data_baixa']
        data_pm_limit = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-1-1' % self.year
        #static_search_params_original = [
        #    ('propietari', '=', True),
        #    '|', ('data_pm', '=', False), ('data_pm', '<', data_pm_limit),
        #    '|', ('data_baixa', '>', data_baixa), ('data_baixa', '=', False),
        #                        ]

        static_search_params = [
            ('propietari', '=', True),
            '|', ('data_pm', '=', False), ('data_pm', '<', data_pm_limit),
        ]
        #print 'static_search_params:{}'.format(static_search_params)
        # Revisem que si està de baixa ha de tenir la data informada.
        static_search_params += [
            '|',
            '&', ('active', '=', False), ('data_baixa', '!=', False),
            ('active', '=', True)
        ]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataAtLinia.read(
                    item, ['trams', 'tensio', 'municipi', 'propietari']
                )
                propietari = linia['propietari'] and '1' or '0'
                search_params = [('linia', '=', linia['id'])]
                search_params += static_search_params
                ids = O.GiscedataAtTram.search(
                    search_params, 0, 0, False, {'active_test': False})
                id_desconegut = O.GiscedataAtCables.search(
                    [('name', '=', 'DESCONEGUT')])

                if not id_desconegut:
                    id_desconegut = O.GiscedataAtCables.search(
                    [('name', '=', 'DESCONOCIDO')])[0]
                for tram in O.GiscedataAtTram.read(ids, fields_to_read):
                    # Comprovar el tipus del cable
                    if 'cable' in tram:
                        cable = O.GiscedataAtCables.read(tram['cable'][0],
                                                     ['tipus'])
                        if not self.embarrats and cable['tipus']:
                            tipus = O.GiscedataAtTipuscable.read(
                                        cable['tipus'][0], ['codi']
                            )
                            # Si el tram tram es embarrat no l'afegim
                            if tipus['codi'] == 'E':
                                continue
                    else:
                        cable = O.GiscedataAtCables.read(
                            id_desconegut, ['tipus'])

                    # Calculem any posada en marxa
                    data_pm = ''
                    if 'data_pm' in tram and tram['data_pm'] and tram['data_pm'] < data_pm_limit:
                        data_pm = datetime.strptime(str(tram['data_pm']),
                                                    '%Y-%m-%d')
                        data_pm = data_pm.strftime('%d/%m/%Y')

                    # Coeficient per ajustar longituds de trams
                    coeficient = tram.get('coeficient',1.0)

                    codi_ccuu = tram.get('cnmc_tipo_instalacion', '')

                    #Agafem la tensió
                    if 'tensio_max_disseny' in tram :
                        tensio = tram['tensio_max_disseny'] / 1000.0
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

                    #Capacitat
                    if 'intensitat_admisible' in cable:
                        cap = (cable['intensitat_admisible'] * tensio *
                               math.sqrt(3) / 1000.0)
                    else:
                        cap = 0

                    if cap < 1:
                        capacitat = 1
                    else:
                        capacitat = int(round(cap))

                    #Descripció
                    origen = tallar_text(tram['origen'], 50)
                    final = tallar_text(tram['final'], 50)
                    if 'longitud_cad' in tram:
                        longitud = round(tram['longitud_cad'] * coeficient/ 1000.0, 3) or 0.001
                    else:
                        longitud = 0
                    if not origen or not final:
                        res = O.GiscegisEdge.search(
                            [('id_linktemplate', '=', tram['name']),
                             ('layer', 'not ilike', self.layer),
                             ('layer', 'not ilike', 'EMBARRA%BT%')
                             ])
                        if not res or len(res) > 1:
                            edge = {'start_node': (0, '%s_0' % tram.get('name')),
                                    'end_node': (0, '%s_1' % tram.get('name'))}
                        else:
                            edge = O.GiscegisEdge.read(res[0], ['start_node',
                                                                'end_node'])
                    if tram.get('data_baixa'):
                        #print 'data_baixa:{}'.format(data_baixa)
                        if tram.get('data_baixa') < data_baixa:
                            fecha_baja = tram.get('data_baixa')
                            #print 'tram_name:{} data_baixa:{}'.format(tram['name'],tram.get('data_baixa'))
                        else:
                            fecha_baja = ''
                            #print 'tram_name:{} data_baixa:{}'.format(tram['name'],tram.get('data_baixa'))
                    else:
                       fecha_baja = ''
                    if data_pm:
                        dpm = time.strptime(data_pm, '%d/%m/%Y')
                        db = time.strptime(data_baixa, '%Y-%m-%d')
                        if dpm > db:
                            estado = 2
                        else:

                            estado = 0
                    else:
                        print 'name:{} data_pm:{}'.format(tram['name'],data_pm)
                        estado = 0

                    output = [
                        'A%s' % tram['name'],
                        tram.get('cini', '') or '',
                        origen or edge['start_node'][1],
                        final or edge['end_node'][1],
                        codi_ccuu or '',
                        comunitat,
                        comunitat,
                        format_f(round(100 - int(tram.get('perc_financament', 0) or 0))),
                        data_pm,
                        fecha_baja or '',
                        tram.get('circuits', 1) or 1,
                        1,
                        format_f(tensio),
                        format_f(longitud, 3),
                        format_f(cable.get('intensitat_admisible', 0) or 0),
                        format_f(cable.get('seccio', 0) or 0),
                        capacitat,
                        propietari,
                        estado
                    ]

                    self.output_q.put(output)

            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
