#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Equips de fiabilitat
"""
from __future__ import absolute_import
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.models import F7Res4666


class FIA(MultiprocessBased):
    """
    Class that generates the fiabilidad(7) file of the 4666
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(FIA, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies FIA'
        self.report_name = 'CNMC INVENTARI FIA'
        self.compare_filed = kwargs["compare_field"]

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        """
        search_params = [('inventari', '=', 'fiabilitat')]
        data_pm = '{0}-01-01' .format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
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
        search_params += [("cini", "not like", "I28")]
        return self.connection.GiscedataCellesCella.search(
            search_params, 0, 0, False, {'active_test': False})

    def consumer(self):
        """
        Method that generates the csb file
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'tipus_element', 'tipus_instalacio_cnmc_id',
            'installacio', 'data_pm', 'data_baixa', 'tram_id',
            self.compare_filed
        ]
        data_pm_limit= '{0}-01-01' .format(self.year + 1)
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                cll = O.GiscedataCellesCella.read(item, fields_to_read)

                #Comprovar si es tipus fiabilitat
                if cll['tipus_instalacio_cnmc_id']:
                    id_cll = cll['tipus_instalacio_cnmc_id'][0]
                    codigo_ccuu = O.GiscedataTipusInstallacio.read(
                        id_cll,
                        ['name'])['name']
                else:
                    codigo_ccuu = ''


                #Instal·lació a la que pertany
                cllinst = cll['installacio'].split(',')

                data_pm = ''
                if cll['data_pm']:
                    data_pm_ct = datetime.strptime(str(cll['data_pm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_ct.strftime('%d/%m/%Y')

                municipi = ''
                provincia = ''
                zona = ''

                #Per trobar la comunitat autonoma
                ccaa = ''
                element_act = ''
                #Comprovo si la cella pertany a ct o lat per trobar la ccaa
                if cll['tram_id']:
                    tram_id = cll['tram_id'][0]
                    element_act = 'A{0}'.format(O.GiscedataAtTram.read(tram_id, ['name'])['name'])

                if cllinst[0] == 'giscedata.cts':
                    ct_vals = O.GiscedataCts.read(int(cllinst[1]), [
                        'id_municipi', 'name', 'id_provincia', 'zona_id'
                    ])
                    if ct_vals['id_municipi']:
                        id_municipi, municipi = ct_vals['id_municipi']
                    if not cll['tram_id']:
                        element_act = ct_vals['name']
                    if ct_vals['id_provincia']:
                        provincia = ct_vals['id_provincia'][1]
                    if ct_vals['zona_id']:
                        zona = ct_vals['zona_id'][1]

                elif cllinst[0] == 'giscedata.at.suport':
                    linia_vals = O.GiscedataAtSuport.read(int(cllinst[1]),
                                                          ['linia'])
                    linia_id = int(linia_vals['linia'][0])
                    lat_vals = O.GiscedataAtLinia.read(linia_id, [
                        'municipi', 'provincia'
                    ])
                    if lat_vals['municipi']:
                        id_municipi, municipi = lat_vals['municipi']
                    if lat_vals['provincia']:
                        provincia = lat_vals['provincia'][1]

                if id_municipi:
                    ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi(
                        id_municipi)[0]
                    ccaa = str(ccaa)
                    while len(ccaa) < 2:
                        ccaa = '0' + ccaa

                if cll['data_baixa']:
                    if cll['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            cll['data_baixa'], '%Y-%m-%d')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')
                    else:
                        fecha_baja = ''
                else:
                    fecha_baja = ''

                if cll[self.compare_filed] and str(self.year + 1) not in str(data_pm):
                    last_data = cll[self.compare_filed]
                    entregada = F7Res4666(**last_data)
                    actual = F7Res4666(
                        cll['name'],
                        cll['cini'],
                        element_act,
                        codigo_ccuu,
                        ccaa,
                        data_pm,
                        fecha_baja,
                        0
                    )
                    if entregada == actual:
                        estado = '0'
                    else:
                        estado = '1'
                else:
                    estado = '2'

                output = [
                    '{0}'.format(cll['name']),
                    cll['cini'] or '',
                    element_act,
                    codigo_ccuu or '',
                    ccaa or '',
                    data_pm,
                    fecha_baja,
                    estado,
                    provincia,
                    municipi,
                    zona
                ]
                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
