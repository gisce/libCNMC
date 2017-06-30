#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
from datetime import datetime
import traceback
import math

from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_id_expedient, tallar_text


class LAT(MultiprocessBased):
    def __init__(self, **kwargs):
        super(LAT, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies AT'
        self.report_name = 'CNMC INVENTARI AT'

    def get_sequence(self):
        search_params = [('name', '!=', '1'), ('propietari', '=', True)]
        return self.connection.GiscedataAtLinia.search(search_params)

    def consumer(self):
        O = self.connection
        fields_to_read = ['baixa', 'data_pm', 'data_industria',
                          'coeficient', 'cini', 'propietari',
                          'tensio_max_disseny', 'name', 'origen',
                          'final', 'perc_financament', 'circuits',
                          'longitud_cad', 'cable', 'expedients_ids']
        data_pm_limit = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        static_search_params = [('propietari', '=', True),
                                '|', ('data_pm', '=', False),
                                     ('data_pm', '<', data_pm_limit),
                                '|', ('data_baixa', '>', data_baixa),
                                     ('data_baixa', '=', False),
                                ]
        # Revisem que si està de baixa ha de tenir la data informada.
        static_search_params += ['|',
                                 '&', ('active', '=', False),
                                      ('data_baixa', '!=', False),
                                 ('active', '=', True)]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataAtLinia.read(
                    item, ['trams', 'tensio', 'municipi']
                )
                search_params = [('id', 'in', linia['trams'])]
                search_params += static_search_params
                ids = O.GiscedataAtTram.search(
                    search_params, 0, 0, False, {'active_test': False})
                for tram in O.GiscedataAtTram.read(ids, fields_to_read):
                    #Comprovar el tipus del cable
                    cable = O.GiscedataAtCables.read(tram['cable'][0],
                                                     ['tipus'])
                    tipus = O.GiscedataAtTipuscable.read(cable['tipus'][0],
                                                         ['codi'])
                    #Si el tram tram es embarrat no l'afegim
                    if tipus['codi'] == 'E':
                        continue

                    # Calculem any posada en marxa
                    data_pm = ''
                    if tram['data_pm'] and tram['data_pm'] < data_pm_limit:
                        data_pm = datetime.strptime(str(tram['data_pm']),
                                                    '%Y-%m-%d')
                        data_pm = data_pm.strftime('%d/%m/%Y')

                    # Coeficient per ajustar longituds de trams
                    coeficient = tram['coeficient'] or 1.0

                    tipus_inst_id = O.Giscedata_cnmcTipo_instalacion.search(
                        [('cini', '=', tram['cini'])])
                    codigo = O.Giscedata_cnmcTipo_instalacion.read(
                        tipus_inst_id, ['codi'])
                    if codigo:
                        codi = codigo[0]
                    else:
                        codi = {'codi': ' '}

                    #Agafem la tensió
                    tensio = tram['tensio_max_disseny'] or linia['tensio']

                    comunitat = ''
                    if linia['municipi']:
                        id_comunitat = O.ResComunitat_autonoma.get_ccaa_from_municipi(
                            linia['municipi'][0])
                        comunidad = O.ResComunitat_autonoma.read(id_comunitat,
                                                                 ['codi'])
                        if comunidad:
                            comunitat = comunidad[0]['codi']

                    # Agafem el cable de la linia
                    cable = O.GiscedataAtCables.read(tram['cable'][0], [
                        'intensitat_admisible', 'seccio'])

                    #Capacitat
                    cap = round(
                        (cable['intensitat_admisible'] * tensio *
                         math.sqrt(3))/1000000, 3)
                    if cap < 1:
                        capacitat = 1
                    else:
                        capacitat = int(round(cap))

                    #Descripció
                    origen = tallar_text(tram['origen'], 50)
                    final = tallar_text(tram['final'], 50)

                    output = [
                        'A%s' % tram['name'],
                        tram['cini'] or '',
                        origen or '',
                        final or '',
                        codi['codi'] or '',
                        comunitat,
                        comunitat,
                        round(100 - int(tram['perc_financament'])),
                        data_pm,
                        '',
                        tram['circuits'] or 1,
                        1,
                        round(tram['longitud_cad'] * coeficient / 1000.0,
                              3) or 0,
                        cable['seccio'],
                        capacitat
                    ]

                    self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()