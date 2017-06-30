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
        data_baixa = '%s-12-31' % self.year
        search_params += [('propietari', '=', True),
                          '|', ('data_pm', '=', False),
                               ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                               ('data_baixa', '=', False)
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataCellesCella.search(
            search_params, 0, 0, False, {'active_test': False})

    def consumer(self):
        O = self.connection
        fields_to_read = ['name', 'cini', 'tipus_element',
                          'cnmc_tipo_instalacion', 'installacio', 'data_pm']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                cll = O.GiscedataCellesCella.read(item, fields_to_read)

                #Comprovar si es tipus fiabilitat
                if cll['tipus_element']:

                    cllt = O.GiscedataCellesTipusElement.read(
                        cll['tipus_element'][0], ['name'])

                codi = cll['cnmc_tipo_instalacion']

                #Instal·lació a la que pertany
                cllinst = cll['installacio'].split(',')

                data_pm = ''
                if cll['data_pm']:
                    data_pm_ct = datetime.strptime(str(cll['data_pm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_ct.strftime('%d/%m/%Y')

                #Per trobar la comunitat autonoma
                ccaa = ''
                element_act = ''
                #Comprovo si la cella pertany a ct o lat per trobar la ccaa
                if cllinst[0] == 'giscedata.cts':
                    ct_vals = O.GiscedataCts.read(int(cllinst[1]),
                                                  ['id_municipi', 'name'])
                    if ct_vals['id_municipi']:
                        id_municipi = ct_vals['id_municipi'][0]
                    element_act = ct_vals['name']

                elif cllinst[0] == 'giscedata.at.suport':
                    linia_vals = O.GiscedataAtSuport.read(int(cllinst[1]),
                                                          ['linia'])
                    linia_id = int(linia_vals['linia'][0])
                    linia_name = linia_vals['linia'][1]
                    lat_vals = O.GiscedataAtLinia.read(linia_id, ['municipi'])
                    if lat_vals['municipi']:
                        id_municipi = lat_vals['municipi'][0]
                    element_act = linia_name

                if id_municipi:
                    ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi(
                        id_municipi)[0]

                output = [
                    '%s' % cll['name'],
                    cll['cini'] or '',
                    element_act,
                    codi or '',
                    ccaa or '',
                    data_pm,
                ]
                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()