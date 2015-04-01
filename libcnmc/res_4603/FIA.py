#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Equips de fiabilitat
"""
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_id_expedient


class FIA(MultiprocessBased):
    def __init__(self, **kwargs):
        super(FIA, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies FIA'
        self.report_name = 'CNMC INVENTARI FIA'

    def get_sequence(self):
        search_params = [('inventari', '=', 'fiabilitat')]
        data_pm = '%s-01-01' % (self.year + 1)
        search_params += [('propietari', '=', True),
                          '|', ('data_pm', '=', False),
                               ('data_pm', '<', data_pm)]
        return self.connection.GiscedataCellesCella.search(search_params)

    def consumer(self):
        O = self.connection
        fields_to_read = ['name', 'cini', 'data_baixa', 'tipus_element',
                          'installacio', 'expedients', 'data_pm']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                cll = O.GiscedataCellesCella.read(item, fields_to_read)

                #Comprovar si es tipus fiabilitat
                if cll['tipus_element']:

                    cllt = O.GiscedataCellesTipusElement.read(
                        cll['tipus_element'][0], ['name'])

                if cll['cini']:
                    #Busquem per la penúltima lletra
                    pos_cini = cll['cini'][5]
                    if pos_cini == '1':
                        codi = 174
                    elif pos_cini == '2':
                        codi = 177
                    elif pos_cini == '3':
                        codi = 179
                    elif pos_cini == '4':
                        codi = 181
                    elif pos_cini == '5':
                        codi = 182
                    elif pos_cini == '6':
                        codi = 183
                    elif pos_cini == '7':
                        codi = 187
                    else:
                        codi = 0
                else:
                    codi = 0

                #Instal·lació a la que pertany
                cllinst = cll['installacio'].split(',')

                data_pm = ''
                if cll['data_pm']:
                    data_pm_ct = datetime.strptime(str(cll['data_pm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_ct.strftime('%d/%m/%Y')

                #Per trobar la comunitat autonoma
                ccaa = ''
                #Comprovo si la cella pertany a ct o lat per trobar la ccaa
                if cllinst[0] == 'giscedata.cts':
                    ct_vals = O.GiscedataCts.read(int(cllinst[1]),
                                                  ['id_municipi'])
                    if ct_vals['id_municipi']:
                        id_municipi = ct_vals['id_municipi'][0]

                elif cllinst[0] == 'giscedata.at.suport':
                    id_linia = O.GiscedataAtSuport.read(int(cllinst[1]),
                                                        ['linia'])
                    lat_vals = O.GiscedataAtLinia.read(
                        int(id_linia['linia'][0]), ['municipi'])
                    if lat_vals['municipi']:
                        id_municipi = lat_vals['municipi'][0]

                if id_municipi:
                    ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi(
                        id_municipi)[0]

                output = [
                    '%s' % cll['name'],
                    cll['cini'] or '',
                    cllt['name'] or '',
                    codi or '',
                    ccaa or '',
                    data_pm,
                    cll['data_baixa'] or ''
                ]
                self.output_q.put(output)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()