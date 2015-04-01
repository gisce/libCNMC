#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_id_expedient


class CTS(MultiprocessBased):
    def __init__(self, **kwargs):
        super(CTS, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies CTS'
        self.report_name = 'CNMC INVENTARI CTS'

    def get_sequence(self):
        search_params = [('id_installacio.name', '!=', 'SE')]
        data_pm = '%s-01-01' % (self.year + 1)
        search_params += [('propietari', '=', True),
                          '|', ('data_pm', '=', False),
                               ('data_pm', '<', data_pm)]
        return self.connection.GiscedataCts.search(search_params)

    def consumer(self):
        O = self.connection
        fields_to_read = ['name', 'cini', 'data_pm', 'expedients_ids',
                          'codi_instalacio', 'id_municipi', 'perc_financament',
                          'descripcio']
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
                    #Si no hi ha ct agafem la comunitat del rescompany
                    company_partner = O.ResCompany.read(1, ['partner_id'])
                    if company_partner:
                        address = O.ResPartnerAddress.read(
                            company_partner['partner_id'][0], ['id_municipi'])
                        if address['id_municipi']:
                            id_municipi = address['id_municipi'][0]

                if id_municipi:
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat_codi = comunitat_vals['codi']

                output = [
                    '%s' % ct['name'],
                    ct['cini'] or '',
                    ct['descripcio'] or '',
                    str(ct['codi_instalacio']) or '',
                    comunitat_codi or '',
                    round(100 - int(ct['perc_financament'])),
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