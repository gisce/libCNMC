#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback, psycopg2.extras
from libcnmc.utils import format_f, convert_srid, get_srid
from libcnmc.core import MultiprocessBased
from libcnmc.utils import (
    format_f, get_id_municipi_from_company, get_forced_elements, adapt_diff, convert_srid, get_srid, format_f,
    convert_spanish_date, get_name_ti, format_f_6181, get_codi_actuacio, get_ine
)
from libcnmc.models import F6Res4666


class FB9(MultiprocessBased):

    """
    Class that generates the B9 file of circular 8/2021
    """
    def __init__(self, **kwargs):
        super(FB9, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB9 - Inversiones efectuadas'
        self.base_object = 'Inversiones'
        self.compare_field = '4666_entregada'

    def get_sequence(self):
        codigos_celda = ['1_E_17', '1_F_17', '1_G_17', '1_H_17', '1_I_17', '1_J_17', '1_K_17', '1_L_17', '1_M_17', '1_N_17', '1_O_17', '1_P_17', '1_E_18', '1_F_18', '1_G_18', '1_H_18', '1_I_18', '1_J_18', '1_K_18', '1_L_18', '1_M_18', '1_N_18', '1_O_18', '1_P_18', '1_E_19', '1_F_19', '1_G_19', '1_H_19', '1_I_19', '1_J_19', '1_K_19', '1_L_19', '1_M_19', '1_N_19', '1_O_19', '1_P_19', '1_E_20', '1_F_20', '1_G_20', '1_H_20', '1_I_20', '1_J_20', '1_K_20', '1_L_20', '1_M_20', '1_N_20', '1_O_20', '1_P_20', '1_E_21', '1_F_21', '1_G_21', '1_H_21', '1_I_21', '1_J_21', '1_K_21', '1_L_21', '1_M_21', '1_N_21', '1_O_21', '1_P_21', '1_E_22', '1_F_22', '1_G_22', '1_H_22', '1_I_22', '1_J_22', '1_K_22', '1_L_22', '1_M_22', '1_N_22', '1_O_22', '1_P_22', '1_E_23', '1_F_23', '1_G_23', '1_H_23', '1_I_23', '1_J_23', '1_K_23', '1_L_23', '1_M_23', '1_N_23', '1_O_23', '1_P_23', '1_E_24', '1_F_24', '1_G_24', '1_H_24', '1_I_24', '1_J_24', '1_K_24', '1_L_24', '1_M_24', '1_N_24', '1_O_24', '1_P_24', '1_E_25', '1_F_25', '1_G_25', '1_H_25', '1_I_25', '1_J_25', '1_K_25', '1_L_25', '1_M_25', '1_N_25', '1_O_25', '1_P_25']


        return [1]

    def consumer(self):
        O = self.connection
        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)
        fields_to_read = [
            'id', 'cini', 'name', 'geom', 'vertex', 'data_apm', 'data_baixa', 'municipi', 'data_baixa_parcial',
            'valor_baixa_parcial', 'motivacion', self.compare_field,
        ]
        fields_to_read_obra = [
            'subvenciones_europeas', 'subvenciones_nacionales', 'subvenciones_prtr', 'financiado', 'fecha_baja',
            'cuenta_contable', 'im_ingenieria', 'im_materiales', 'im_obracivil', 'im_trabajos'
        ]

        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)

                # Cuadro 1A

                # LINEAS DE ALTA DISTRIBUCIÓN A

                self.output_q.put([
                    # codigo_celda,               # CODIGO_CELDA
                    # importe,                    # IMPORTE
                    'WIP',
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
