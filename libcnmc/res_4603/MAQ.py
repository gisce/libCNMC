#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Maquines
"""
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased


class MAQ(MultiprocessBased):
    def __init__(self, **kwargs):
        super(MAQ, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies MAQ'
        self.report_name = 'CNMC INVENTARI MAQ'

    def get_sequence(self):
        search_params = ['|', ('id_estat.cnmc_inventari', '=', True),
                         ('id_estat', '=', False)]
        return self.connection.GiscedataTransformadorTrafo.search(search_params)

    def consumer(self):
        O = self.connection
        fields_to_read = ['cini', 'historic', 'data_pm', 'ct', 'name',
                          'potencia_nominal', 'codi_instalacio',
                          'numero_fabricacio']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                trafo = O.GiscedataTransformadorTrafo.read(
                    item, fields_to_read)

                if trafo['codi_instalacio']:
                    codi = trafo['codi_instalacio']
                else:
                    codi = ''

                if trafo['historic']:
                    historic = O.GiscedataTransformadorHistoric.read(
                        trafo['historic'][0], ['data_entrada'])
                    data_hist = historic['data_entrada']
                    for hist_id in trafo['historic']:
                        historic = O.GiscedataTransformadorHistoric.read(
                            hist_id, ['data_entrada'])
                        if historic['data_entrada'] < data_hist:
                            data_hist = historic['data_entrada']
                else:
                    data_hist = ''
                data_pm = trafo['data_pm'] or data_hist
                if data_pm:
                    data_pm = datetime.strptime(str(data_pm),
                                                '%Y-%m-%d %H:%M:%S')
                    data_pm = data_pm.strftime('%d/%m/%Y')

                comunitat = ''
                c_ccaa = ''
                finan = 0
                if trafo['ct']:
                    cts = O.GiscedataCts.read(trafo['ct'][0],
                                              ['id_municipi',
                                               'perc_financament'])
                    id_comunitat = O.ResComunitat_autonoma.\
                        get_ccaa_from_municipi([], cts['id_municipi'][0])
                    comunidad = O.ResComunitat_autonoma.read(id_comunitat,
                                                             ['codi'])
                    if comunidad:
                        comunitat = comunidad[0]['codi']
                    finan = cts['perc_financament']

                #La propia empresa
                if O.ResCompany.get(1):
                    company_partner = O.ResCompany.read(1, ['partner_id'])
                    address = O.ResPartnerAddress.read(
                        company_partner['partner_id'][0], ['municipi'])
                    id_comunitat = O.ResComunitat_autonoma.get_ccaa_from_municipi(
                        [], address['municipi'][0])
                    c_ccaa = O.ResComunitat_autonoma.read(id_comunitat,
                                                          ['codi'])
                    c_ccaa = c_ccaa[0]['codi']
                output = [
                    '%s' % trafo['name'],
                    trafo['cini'] or '',
                    trafo['numero_fabricacio'] or '',
                    codi or '',
                    '',
                    comunitat or c_ccaa or '',
                    round(100 - int(finan)),
                    data_pm,
                    '',
                    trafo['potencia_nominal']
                ]

                self.output_q.put(output)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()