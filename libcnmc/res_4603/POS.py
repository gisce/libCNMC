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
        search_params = []
        return self.connection.GiscedataCtsSubestacionsPosicio.search(
            search_params)

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
                    if not QUIET:
                        sys.stderr.write("**** ERROR: El ct %s (id:%s) no està "
                                         "en giscedata_cts_subestacions_"
                                         "posicio.\n"
                                         % (sub['name'], sub['id']))
                        sys.stderr.flush()

                # Calculem any posada en marxa
                data_pm = sub['data_pm']
                if data_pm:
                    data_pm = datetime.strptime(str(data_pm), '%Y-%m-%d')
                    data_pm = data_pm.strftime('%d/%m/%Y')

                #Codi tipus de instalació
                codi = sub['codi_instalacio']

                ccaa = ''
                c_ccaa = ''
                #La propia empresa
                company = O.ResCompany.get(1)

                cts = O.GiscedataCtsSubestacions.read(sub['subestacio_id'][0],
                                                      ['id_municipi',
                                                       'descripcio'])
                if cts:
                    municipi = O.ResMunicipi.read(cts['id_municipi'][0],
                                                  ['state'])
                    if municipi['state']:
                        provincia = O.ResCountryState.read(
                            municipi['state'][0],
                            ['comunitat_autonoma'])
                        if provincia['comunitat_autonoma']:
                            ccaa = provincia['comunitat_autonoma'][0]

                if company.partner_id.address[0].state_id:
                    c_ccaa = company.partner_id.address[0].\
                        state_id.comunitat_autonoma.codi

                output = [
                    '%s' % sub['name'],
                    sub['cini'] or ' ',
                    cts['descripcio'] or ' ',
                    codi,
                    ccaa or c_ccaa or ' ',
                    round(100 - int(sub['perc_financament'])),
                    data_pm or ' ',
                    ' '
                ]

                self.output_q.put(output)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
