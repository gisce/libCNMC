#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
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
        search_params = [('data_inici', '=', self.year), ('data_final', '=', self.year)]
        return self.connection.GiscedataTasaOcupacioVp.search(search_params)

    def get_ine(self, municipi_id):
        O = self.connection
        muni = O.ResMunicipi.read(municipi_id, ['ine', 'dc'])
        return get_ine(O, muni['ine'])

    def consumer(self):
        O = self.connection

        fields_to_read = ['municipi_id', 'imu_total_anual']

        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                tovp = O.GiscedataTasaOcupacioVp.read(item, fields_to_read)

                # MUNICIPIO I PROVINCIA
                municipio = ''
                provincia = ''
                if tovp.get('municipi_id', False):
                    provincia, municipio = self.get_ine(tovp['municipi_id'][0])

                # CCAA
                # funció per trobar la ccaa desde el municipi
                id_municipi = ''
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                if tovp['municipi_id']:
                    id_municipi = tovp['municipi_id'][0]
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
                imu_total_anual = ''
                if tovp.get('imu_total_anual', False):
                    imu_total_anual = tovp['imu_total_anual']

                self.output_q.put([
                    municipio,                                  # MUNICIPIO
                    provincia,                                  # PROVINCIA
                    comunitat_codi,                             # CCAA
                    format_f(imu_total_anual, decimals=2),      # TOVP
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
