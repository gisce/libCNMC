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
        search_params = [('id_installacio.name', '!=', 'SE')]

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

                comunitat = ''
                data_pm = ''

                #Busco la data, primer mirer els expedients, sino la data_pm CT
                if ct['expedients_ids']:
                    try:
                        for exp_id in ct['expedients_ids']:
                            exp = O.GiscedataExpedientsExpedient.read(
                                exp_id, ['industria_data'])
                            if exp['industria_data']:
                                data_industria = exp['industria_data']
                                break
                        data_industria = datetime.strptime(str(data_industria),
                                                           '%Y-%m-%d')
                        data_pm = data_industria.strftime('%d/%m/%Y')
                    except Exception as e:
                        print "Data d'expedient no trobada, CT id: %d, %s" % (
                            item, e)
                        if ct['data_pm']:
                            data_pm_ct = datetime.strptime(str(ct['data_pm']),
                                                           '%Y-%m-%d')
                            data_pm = data_pm_ct.strftime('%d/%m/%Y')
                else:
                    if ct['data_pm']:
                        data_pm_ct = datetime.strptime(str(ct['data_pm']),
                                                       '%Y-%m-%d')
                        data_pm = data_pm_ct.strftime('%d/%m/%Y')

                #funció per trobar la ccaa desde el municipi
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi

                if ct['id_municipi']:
                    id_comunitat = fun_ccaa([], ct['id_municipi'][0])
                    comunidad = O.ResComunitat_autonoma.read(id_comunitat,
                                                             ['codi'])
                    if comunidad:
                        comunitat = comunidad[0]['codi']
                else:
                    #Si no hi ha ct agafem la comunitat del rescompany
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
                    '%s' % ct['name'],
                    ct['cini'] or '',
                    ct['descripcio'] or '',
                    str(ct['codi_instalacio']) or '',
                    comunitat,
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