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
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                cll = O.GiscedataCellesCella.get(item)

                #Comprovar si es tipus fiabilitat
                if cll.tipus_element:
                    cllt = O.GiscedataCellesTipusElement.get(
                        int(cll.tipus_element.name))
                    tipus_inst_id = O.Giscedata_cnmcTipo_instalacion.search(
                        [('cini', '=', cll.cini)])
                    codigo = O.Giscedata_cnmcTipo_instalacion.read(
                        tipus_inst_id, ['codi'])
                    if codigo:
                        codi = codigo[0]
                    else:
                        codi = {'codi': ' '}
                else:
                    cllt = {'name': ''}
                    codi = {'codi': ' '}

                try:
                    data_industria = ''
                    if cll.expedients:
                        for exp in cll.expedients_ids:
                            if exp.industria_data:
                                data_industria = exp.industria_data
                                break
                    if data_industria:
                        data_industria = datetime.strptime(str(data_industria),
                                                           '%Y-%m-%d')
                        data_aps = data_industria.strftime('%d/%m/%Y')
                    else:
                        data_pm = datetime.strptime(str(cll.data_pm),
                                                    '%Y-%m-%d')
                        data_aps = data_pm.strftime('%d/%m/%Y')
                except:
                    data_aps = ''

                #Per trobar la comunitat autonoma
                ccaa = ''
                #Comprovo si la cella pertany a ct o lat
                cllinst = cll.installacio.split(',')
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
                    '%s' % cll.name,
                    cll.cini or '',
                    cllt.name or '',
                    codi['codi'] or '',
                    ccaa or '',
                    data_aps,
                    cll.data_baixa or ''
                ]
                self.output_q.put(output)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()