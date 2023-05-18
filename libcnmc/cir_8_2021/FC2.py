#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC - TOVP
"""
from __future__ import absolute_import
from datetime import datetime
import traceback, psycopg2.extras
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f, get_id_municipi_from_company, get_ine


class FC2(StopMultiprocessBased):

    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        super(FC2, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FC2 - TOVP'
        self.base_object = 'TOVP'
        self.compare_field = '4666_entregada'

    def get_sequence(self):
        c2_ids = self.connection.model('cir8.2021.c2').search([('year', '=', self.year)])
        return c2_ids

    def get_ine(self, municipi_id):
        O = self.connection
        muni = O.ResMunicipi.read(municipi_id, ['ine', 'dc'])
        return get_ine(O, muni['ine'])

    def consumer(self):
        O = self.connection

        fields_to_read = ['municipio', 'tovp']

        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                c2 = O.model('cir8.2021.c2').read(item, fields_to_read)

                # MUNICIPIO I PROVINCIA
                municipio = ''
                provincia = ''
                if c2.get('municipi_id', False):
                    provincia, municipio = self.get_ine(c2['municipio'][0])

                # CCAA
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                if c2['municipio']:
                    id_municipi = c2['municipi_id'][0]
                else:
                    id_municipi = get_id_municipi_from_company(O)

                comunitat_codi = ''
                if id_municipi:
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat_codi = comunitat_vals['codi']

                # TOVP
                tovp = '0,000'
                if c2.get('tovp', False):
                    tovp = c2['tovp']

                self.output_q.put([
                    municipio,                                  # MUNICIPIO
                    provincia,                                  # PROVINCIA
                    comunitat_codi,                             # CCAA
                    format_f(tovp, decimals=2),                 # TOVP
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
