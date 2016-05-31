#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f, get_id_municipi_from_company
from libcnmc.models.f8_4771 import F8Res4771


class CTS(MultiprocessBased):
    """
    Class that generates the CT file of the 4131
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(CTS, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies CTS'
        self.report_name = 'CNMC INVENTARI CTS'

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        """

        search_params = [('id_installacio.name', '!=', 'SE')]
        data_pm = '{0}-01-01'.format(self.year + 1)
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
        return self.connection.GiscedataCts.search(
            search_params, 0, 0, False, {'active_test': False})

    def consumer(self):
        """
        Method that generates the csb file
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'data_pm', 'cnmc_tipo_instalacion', 'id_municipi',
            'perc_financament', 'descripcio', 'data_baixa', '4771_entregada'
        ]
        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                ct = O.GiscedataCts.read(item, fields_to_read)

                comunitat_codi = ''
                data_pm = ''

                if ct['data_pm']:
                    data_pm_ct = datetime.strptime(str(ct['data_pm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_ct.strftime('%d/%m/%Y')

                #funció per trobar la ccaa desde el municipi
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi

                if ct['id_municipi']:
                    id_municipi = ct['id_municipi'][0]
                else:
                    id_municipi = get_id_municipi_from_company(O)

                if id_municipi:
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat_codi = comunitat_vals['codi']

                if ct['data_baixa']:
                    if ct['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            ct['data_baixa'], '%Y-%m-%d %H:%M:%S')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')
                    else:
                        fecha_baja = ''
                else:
                    fecha_baja = ''

                if ct['4771_entregada']:
                    data_4771 = ct['4771_entregada']
                    entregada = F8Res4771(**data_4771)
                    actual = F8Res4771(
                        ct['name'],
                        ct['cini'],
                        ct['descripcio'],
                        str(ct['cnmc_tipo_instalacion']),
                        comunitat_codi,
                        format_f(round(100 - int(ct['perc_financament']))),
                        fecha_baja,
                    )
                    if entregada == actual:
                        estado = '0'
                    else:
                        estado = '1'
                else:
                    estado = '2'

                output = [
                    '{0}'.format(ct['name']),
                    ct['cini'] or '',
                    ct['descripcio'] or '',
                    str(ct['cnmc_tipo_instalacion']) or '',
                    comunitat_codi or '',
                    format_f(round(100 - int(ct['perc_financament']))),
                    data_pm,
                    fecha_baja,
                    estado
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
