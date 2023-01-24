#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
CNMC - Importes devengados
"""
from __future__ import absolute_import
from datetime import datetime
import traceback, psycopg2.extras
from libcnmc.utils import format_f, convert_srid, get_srid
from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_ine
from libcnmc.models import F6Res4666


class FC2(MultiprocessBased):

    """
    Class that generates the C2 file about 'Importes devengados'
    """
    def __init__(self, **kwargs):
        super(FC2, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB8 - Otros activos'
        self.base_object = 'Despatxos'
        self.compare_field = '4666_entregada'

    def get_sequence(self):
        sql = """
        SELECT DISTINCT m.id
        FROM res_municipi m 
        JOIN giscedata_cups_ps c 
        ON c.id_municipi = m.id
        """

        return self.connection.ResMunicipi.search()

    def get_ine(self, municipi_id):
        O = self.connection
        muni = O.ResMunicipi.read(municipi_id, ['ine', 'dc'])
        return get_ine(O, muni['ine'])

    def consumer(self):
        O = self.connection

        fields_to_read = ['tases_ocupacio_ids']

        while True:
            try:
                # Generar línies
                item = self.input_q.get()
                self.progress_q.put(item)
                municipi = O.ResMunicipi.read(item, fields_to_read)

                # MUNICIPI I PROVINCIA
                municipio = ''
                provincia = ''
                provincia, municipio = self.get_ine(item)

                # CCAA
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                ccaa = ''
                id_comunitat = fun_ccaa(item)
                comunitat_vals = O.ResComunitat_autonoma.read(id_comunitat[0], ['codi'])
                if comunitat_vals:
                    ccaa = comunitat_vals['codi']

                # TOVP
                inici_any = '{}-01-01'.format(self.year)
                fi_any = '{}-12-31'.format(self.year)
                tovp = 0
                if municipi.get('tases_ocupacio_ids', False):
                    tasa_ids = municipi['tases_ocupacio_ids']
                    tasa_obj = O.GiscedataTasaOcupacioVp
                    tasa_data = tasa_obj.read(tasa_ids, ['data_inici', 'data_final', 'imu_total_anual'])
                    for tasa in tasa_data:
                        if inici_any <= tasa['data_inici'] <= fi_any and inici_any <= tasa['data_final'] <= fi_any:
                            tovp += tasa['imu_total_anual']

                output = [
                    municipio,          # MUNICIPIO
                    provincia,          # PROVINCIA
                    ccaa,               # CCAA
                    tovp,               # TOVP
                ]
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
