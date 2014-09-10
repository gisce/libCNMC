#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased


class CTS(MultiprocessBased):
    def __init__(self, **kwargs):
        super(CTS, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies CTS'
        self.report_name = 'CNMC INVENTARI CTS'

    def get_sequence(self):
        search_params = [('name', '!=', '1'),
                         ('id_installacio.name', '!=', 'SE')]

        return self.connection.GiscedataCts.search(search_params)

    def consumer(self):
        O = self.connection
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                ct = O.GiscedataCts.get(item)
                codi = ct.codi_instalacio
                if codi == 0:
                    codi = '0'

                #Busco la data, primer mirer els expedients, sino la data_pm CT
                if ct.expedients_ids:
                    try:
                        for exp in ct.expedients_ids:
                            if exp.industria_data:
                                data_industria = exp.industria_data
                                break
                        data_industria = datetime.strptime(str(data_industria),
                                                           '%Y-%m-%d')
                        data_pm = data_industria.strftime('%d/%m/%Y')
                    except Exception as e:
                        print "error: %d %s" % (item, e)
                        if ct.data_pm:
                            data_pm_ct = datetime.strptime(str(ct.data_pm),
                                                           '%Y-%m-%d')
                            data_pm = data_pm_ct.strftime('%d/%m/%Y')
                else:
                    if ct.data_pm:
                        data_pm_ct = datetime.strptime(str(ct.data_pm),
                                                       '%Y-%m-%d')
                        data_pm = data_pm_ct.strftime('%d/%m/%Y')


                ccaa = ct.id_municipi.state.comunitat_autonoma.codi
                finan = ct.perc_financament

                output = [
                    '%s' % ct.name,
                    ct.cini or '',
                    ct.descripcio or '',
                    codi or '',
                    ccaa or '',
                    round(100 - int(finan)),
                    data_pm,
                    ''
                ]

                self.output_q.put(output)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()