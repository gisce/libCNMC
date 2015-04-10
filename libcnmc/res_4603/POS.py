#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Posicions
"""
from datetime import datetime
import traceback
import sys

from libcnmc.core import MultiprocessBased

QUIET = False


class POS(MultiprocessBased):
    def __init__(self, **kwargs):
        super(POS, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies POS'
        self.report_name = 'CNMC INVENTARI POS'

    def get_sequence(self):
        search_params = [('interruptor', '=', '2')]
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params += [('propietari', '=', True),
                          '|', ('data_pm', '=', False),
                               ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                               ('data_baixa', '=', False),
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataCtsSubestacionsPosicio.search(
            search_params, 0, 0, False, {'active_test': False})

    def consumer(self):
        O = self.connection
        fields_to_read = ['name', 'cini', 'data_pm', 'subestacio_id',
                          'codi_instalacio', 'perc_financament']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                sub = O.GiscedataCtsSubestacionsPosicio.read(
                    item, fields_to_read)
                if not sub:
                    txt = ('**** ERROR: El ct %s (id:%s) no està a '
                           'giscedata_cts_subestacions_posicio.\n' %
                           (sub['name'], sub['id']))
                    if not QUIET:
                        sys.stderr.write(txt)
                        sys.stderr.flush()

                    raise Exception(txt)

                # Calculem any posada en marxa
                data_pm = sub['data_pm']
                if data_pm:
                    data_pm = datetime.strptime(str(data_pm), '%Y-%m-%d')
                    data_pm = data_pm.strftime('%d/%m/%Y')

                #Codi tipus de instalació
                codi = sub['codi_instalacio']

                comunitat = ''

                cts = O.GiscedataCtsSubestacions.read(sub['subestacio_id'][0],
                                                      ['id_municipi',
                                                       'descripcio'])
                if cts['id_municipi']:
                    id_municipi = cts['id_municipi'][0]
                else:
                    #Si no hi ha ct agafem la comunitat del rescompany
                    company_partner = O.ResCompany.read(1, ['partner_id'])
                    #funció per trobar la ccaa desde el municipi
                    if company_partner:
                        address = O.ResPartnerAddress.read(
                            company_partner['partner_id'][0], ['id_municipi'])
                        if address['id_municipi']:
                            id_municipi = address['id_municipi'][0]

                if id_municipi:
                    #funció per trobar la ccaa desde el municipi
                    fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat = comunitat_vals['codi']

                output = [
                    '%s' % sub['name'],
                    sub['cini'] or '',
                    cts['descripcio'] or '',
                    codi,
                    comunitat,
                    round(100 - int(sub['perc_financament'])),
                    data_pm or '',
                    ''
                ]

                self.output_q.put(output)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
