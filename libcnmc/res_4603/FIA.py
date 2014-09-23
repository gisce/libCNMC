#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Equips de fiabilitat
"""
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased


class FIA(MultiprocessBased):
    def __init__(self, **kwargs):
        super(FIA, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies FIA'
        self.report_name = 'CNMC INVENTARI FIA'

    def get_sequence(self):
        search_params = [('inventari', '=', 'fiabilitat')]
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

                data_industria = ''
                data_pm = ''
                #Busco la data, primer mirer els expedients, sino la data_pm CT
                if cll['expedients']:
                    try:
                        for exp_id in cll['expedients']:
                            exp = O.GiscedataExpedientsExpedient.read(
                                exp_id, ['industria_data'])
                            if exp['industria_data']:
                                data_industria = exp['industria_data']
                                break
                        data_industria = datetime.strptime(str(data_industria),
                                                           '%Y-%m-%d')
                        print ' dins del try %s' % data_pm
                        data_pm = data_industria.strftime('%d/%m/%Y')
                    except Exception as e:
                        print ' dins del except %s' % data_pm
                        print "Data d'expedient no trobada, " \
                              "Cella id: %d, %s" % (item, e)
                        if cll['data_pm']:
                            data_pm_ct = datetime.strptime(str(cll['data_pm']),
                                                           '%Y-%m-%d')
                            data_pm = data_pm_ct.strftime('%d/%m/%Y')
                else:
                    if cll['data_pm']:
                        print ' dins del else %s' % data_pm
                        data_pm_ct = datetime.strptime(str(cll['data_pm']),
                                                       '%Y-%m-%d')
                        data_pm = data_pm_ct.strftime('%d/%m/%Y')

                #Per trobar la comunitat autonoma
                ccaa = ''
                #Comprovo si la cella pertany a ct o lat
                cllinst = cll['installacio'].split(',')
                if cllinst[0] == 'giscedata.cts':
                    id_municipi = O.GiscedataCts.read(int(cllinst[1]),
                                                      ['id_municipi'])
                    if id_municipi:
                        ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi(
                            [], id_municipi['id_municipi'][0])[0]

                elif cllinst[0] == 'giscedata.at.suport':
                    id_linia = O.GiscedataAtSuport.read(int(cllinst[1]),
                                                        ['linia'])
                    id_municipi = O.GiscedataAtLinia.read(
                        int(id_linia['linia'][0]), ['municipi'])
                    if id_municipi:
                        ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi(
                            [], id_municipi['municipi'][0])[0]

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