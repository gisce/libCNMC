#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
from datetime import datetime
import traceback
import math

from libcnmc.core import MultiprocessBased


class LAT(MultiprocessBased):
    def __init__(self, **kwargs):
        super(LAT, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies AT'
        self.report_name = 'CNMC INVENTARI AT'

    def get_sequence(self):
        search_params = [('name', '!=', '1')]
        return self.connection.GiscedataAtLinia.search(search_params)

    def consumer(self):
        O = self.connection
        fields_to_read = ['baixa', 'data_pm', 'data_industria',
                          'coeficient', 'cini',
                          'tensio_max_disseny', 'name', 'origen',
                          'final', 'perc_financament', 'circuits',
                          'longitud_cad', 'cable', 'expedients_ids']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataAtLinia.read(
                    item, ['trams', 'tensio', 'municipi']
                )
                for tram in O.GiscedataAtTram.read(linia['trams'], fields_to_read):
                    #Comprovar el tipus del cable
                    cable = O.GiscedataAtCables.read(tram['cable'][0],
                                                     ['tipus'])
                    tipus = O.GiscedataAtTipuscable.read(cable['tipus'][0],
                                                         ['codi'])
                    #Si el tram tram es embarrat no l'afegim
                    if tipus['codi'] == 'E':
                        continue

                    #Si el tram es de baixa no l'afegim
                    if tram['baixa']:
                        continue
                    # Calculem any posada en marxa
                    if not tram['expedients_ids']:
                        data_pm = tram['data_pm']
                    else:
                        try:
                            #Busco en els expedients la data d'industria
                            for exp in tram['expedients_ids']:
                                data_exp = O.GiscedataExpedientsExpedient.read(
                                    exp, ['industria_data'])
                                break
                            data_pm = data_exp['industria_data'] or ''
                        except:
                            #No s'ha trobat l'expedient
                            data_pm = tram['data_pm']

                    if data_pm:
                        data_pm = datetime.strptime(str(data_pm), '%Y-%m-%d')
                        data_pm = data_pm.strftime('%d/%m/%Y')

                    # Coeficient per ajustar longituds de trams
                    coeficient = tram['coeficient'] or 1.0

                    tipus_inst_id = O.Giscedata_cnmcTipo_instalacion.search(
                        [('cini', '=', tram['cini'])])
                    codigo = O.Giscedata_cnmcTipo_instalacion.read(tipus_inst_id,
                                                                   ['codi'])
                    if codigo:
                        codi = codigo[0]
                    else:
                        codi = {'codi': ' '}

                    #Agafem la tensió
                    tensio = tram['tensio_max_disseny'] or linia['tensio']

                    id_comunitat = O.ResComunitat_autonoma.get_ccaa_from_municipi(
                        [], linia['municipi'][0])
                    comunidad = O.ResComunitat_autonoma.read(id_comunitat, ['codi'])
                    if comunidad:
                        comunitat = comunidad[0]

                    # Agafem el cable de la linia
                    cable = O.GiscedataAtCables.read(tram['cable'][0], [
                        'intensitat_admisible', 'seccio'])

                    output = [
                        'A%s' % tram['name'],
                        tram['cini'] or '',
                        tram['origen'] or '',
                        tram['final'] or '',
                        codi['codi'] or '',
                        comunitat['codi'] or '',
                        comunitat['codi'] or '',
                        round(100 - int(tram['perc_financament'])),
                        data_pm,
                        '',
                        tram['circuits'] or 1,
                        1,
                        round(tram['longitud_cad'] * coeficient / 1000.0,
                              3) or 0, cable['seccio'],
                        round(
                            (cable['intensitat_admisible'] * tensio *
                             math.sqrt(3))/1000000, 3)]

                    self.output_q.put(output)

            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()