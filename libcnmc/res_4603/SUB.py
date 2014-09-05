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
        search_params = [('name', '!=', '1')]
        return self.connection.GiscedataCtsSubestacions.search(search_params)

    def consumer(self):
        O = self.connection
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                sub = O.GiscedataCtsSubestacions.get(item)
                if not sub:
                    if not QUIET:
                        sys.stderr.write("**** ERROR: El ct %s (id:%s) no està "
                                         "en giscedata_cts_subestacions.\n"
                                         % (sub.name, sub.id))
                        sys.stderr.flush()

                # Calculem any posada en marxa
                data_pm = sub.data_pm or sub.data_industria

                if data_pm:
                    data_pm = datetime.strptime(str(data_pm), '%Y-%m-%d')
                    data_pm = data_pm.strftime('%d/%m/%Y')

                c_ccaa = ''
                #La propia empresa
                company = O.ResCompany.get(1)
                ccaa = sub.id_municipi.state.comunitat_autonoma.codi
                if company.partner_id.address[0].state_id:
                    c_ccaa = company.partner_id.address[0].state_id.\
                        comunitat_autonoma.codi

                pos = len(sub.posicions.objects)

                output = [
                    '%s' % sub.name,
                    sub.cini or '',
                    sub.descripcio or '',
                    '',
                    ccaa or c_ccaa or '',
                    round(100 - int(sub.perc_financament)),
                    data_pm,
                    '',
                    pos
                ]

                self.output_q.put(output)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()