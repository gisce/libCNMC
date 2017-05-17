#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Subestacions
"""
from datetime import datetime
import traceback
import sys

from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_id_municipi_from_company

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
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
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
        return self.connection.GiscedataCtsSubestacions.search(
            search_params, 0, 0, False, {'active_test': False})

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
                    txt = ("**** ERROR: El ct %s (id:%s) no està en "
                           "giscedata_cts_subestacions.\n" %
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

                comunitat = ''
                id_municipi = None
                if sub['id_municipi']:
                    id_municipi = sub['id_municipi'][0]
                else:
                    id_municipi = get_id_municipi_from_company(O)

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
                    sub['descripcio'] or '',
                    '',
                    comunitat,
                    round(100 - int(sub['perc_financament'])),
                    data_pm,
                    '',
                    len(sub['posicions'])
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()