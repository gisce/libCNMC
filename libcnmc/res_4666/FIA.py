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
from libcnmc.utils import get_forced_elements


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
        self.extended = kwargs.get("extended", False)

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
        ids = self.connection.GiscedataCellesCella.search(
            search_params, 0, 0, False, {'active_test': False})

        forced_ids = get_forced_elements(
            self.connection,
            "giscedata.celles.cella"
        )

        ids = ids + forced_ids["include"]
        ids = list(set(ids) - set(forced_ids["exclude"]))
        return list(set(ids))

    def consumer(self):
        """
        Method that generates the csb file
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'tipus_element', 'tipus_instalacio_cnmc_id',
            'installacio', 'data_pm', 'data_baixa', 'tram_id',
            "4666_identificador",
            self.compare_filed
        ]
        data_pm_limit= '{0}-01-01' .format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                cll = O.GiscedataCellesCella.read(item, fields_to_read)

                # Comprovar si es tipus fiabilitat
                if cll['tipus_element']:

                    cllt = O.GiscedataCellesTipusElement.read(
                        cll['tipus_element'][0], ['name'])
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

                #Per trobar la comunitat autonoma
                ccaa = ''
                element_act = ''
                #Comprovo si la cella pertany a ct o lat per trobar la ccaa
                if cll['tram_id']:
                    tram_id = cll['tram_id'][0]
                    element_act = 'A{0}'.format(O.GiscedataAtTram.read(tram_id, ['name'])['name'])

                if cllinst[0] == 'giscedata.cts':
                    ct_vals = O.GiscedataCts.read(
                        int(cllinst[1]),
                        ['id_municipi', 'id_provincia', 'zona_id', 'name']
                    )
                    if ct_vals['id_municipi']:
                        id_municipi = ct_vals['id_municipi'][0]
                    if not cll['tram_id']:
                        element_act = ct_vals['name']
                    if self.extended:

                        if 'id_provincia' in ct_vals:
                            provincia = O.ResCountryState.read(
                                ct_vals['id_provincia'][0], ['name']
                            )
                            provincia = provincia.get('name', "")
                        else:
                            provincia = ""

                        if 'id_municipi' in ct_vals:
                            municipi = O.ResMunicipi.read(
                                ct_vals['id_municipi'][0], ['name']
                            )
                            municipi = municipi.get('name', "")
                        else:
                            municipi = ""

                        if 'zona_id' in ct_vals:
                            zona = O.GiscedataCtsZona.read(
                                ct_vals['zona_id'][0], ['name']
                            )
                            zona = zona.get('name', "")
                        else:
                            zona = ""

                        to_append = [provincia, municipi, zona]
                elif cllinst[0] == 'giscedata.at.suport':
                    linia_vals = O.GiscedataAtSuport.read(int(cllinst[1]),
                                                          ['linia'])
                    linia_id = int(linia_vals['linia'][0])
                    lat_vals = O.GiscedataAtLinia.read(
                        linia_id, ['municipi', 'provincia'])
                    if lat_vals['municipi']:
                        id_municipi = lat_vals['municipi'][0]

                    if self.extended:

                        if 'provincia' in lat_vals:
                            provincia = O.ResCountryState.read(
                                lat_vals['provincia'][0], ['name']
                            )
                            provincia = provincia.get('name', "")
                        else:
                            provincia = ""

                        if 'municipi' in lat_vals:
                            municipi = O.ResMunicipi.read(
                                lat_vals['municipi'][0], ['name']
                            )
                            municipi = municipi.get('name', "")
                        else:
                            municipi = ""

                        to_append = [provincia, municipi]

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
                    if cll['data_pm']:
                        if cll['data_pm'][:4] != str(self.year):
                            estado = '1'
                        else:
                            estado = '2'
                    else:
                        estado = '1'

                output = [
                    cll.get("4666_identificador",cll["name"]),
                    cll['cini'] or '',
                    element_act,
                    codigo_ccuu or '',
                    ccaa or '',
                    data_pm,
                    fecha_baja,
                    estado
                ]

                if self.extended:
                    output = output + to_append

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
