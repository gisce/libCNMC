#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Subestacions
"""
from datetime import datetime
import traceback
import sys

from libcnmc.core import MultiprocessBased

QUIET = False


class SUB(MultiprocessBased):
    def __init__(self, **kwargs):
        super(SUB, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Subestacions'
        self.report_name = 'CNMC INVENTARI SUB'

    def get_sequence(self):
        search_params = []
        return self.connection.GiscedataCtsSubestacions.search(search_params)

    def consumer(self):
        O = self.connection
        fields_to_read = ['name', 'data_industria', 'data_pm', 'id_municipi',
                          'posicions', 'cini', 'descripcio', 'perc_financament']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                sub = O.GiscedataCtsSubestacions.read(item, fields_to_read)

                if not sub:
                    if not QUIET:
                        sys.stderr.write("**** ERROR: El ct %s (id:%s) no està "
                                         "en giscedata_cts_subestacions.\n"
                                         % (sub['name'], sub['id']))
                        sys.stderr.flush()

                # Calculem any posada en marxa
                data_pm = sub['data_industria'] or sub['data_pm']

                if data_pm:
                    data_pm = datetime.strptime(str(data_pm), '%Y-%m-%d')
                    data_pm = data_pm.strftime('%d/%m/%Y')

                comunitat = ''
                if sub['id_municipi']:
                    municipi = O.ResMunicipi.read(sub['id_municipi'][0],
                                                  ['state'])
                    if municipi['state']:
                        provincia = O.ResCountryState.read(
                            municipi['state'][0],
                            ['comunitat_autonoma'])
                        if provincia['comunitat_autonoma']:
                            comunitat = provincia['comunitat_autonoma'][0]
                else:
                    #Si no hi ha subestació agafem la comunitat del rescompany
                    company_partner = O.ResCompany.read(1, ['partner_id'])
                    #funció per trobar la ccaa desde el municipi
                    fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                    if company_partner:
                        address = O.ResPartnerAddress.read(
                            company_partner['partner_id'][0], ['id_municipi'])
                        if address['id_municipi']:
                            id_comunitat = fun_ccaa(
                                [], address['id_municipi'][0])
                            comunidad = O.ResComunitat_autonoma.read(
                                id_comunitat, ['codi'])
                            comunitat = comunidad[0]['codi']

                output = [
                    '%s' % sub['name'],
                    sub['cini'] or '',
                    sub['descripcio'] or '',
                    '',
                    comunitat,
                    round(100 - int(sub['perc_financament'])),
                    data_pm,
                    '',
                    len(sub['posicions'])
                ]

                self.output_q.put(output)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()