#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
import pandas as pd
from collections import OrderedDict


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
        codigos_celda = [
            '1_E_17', '1_F_17', '1_G_17', '1_H_17', '1_I_17', '1_J_17', '1_K_17', '1_L_17', '1_M_17', '1_N_17', '1_O_17', '1_P_17',
            '1_E_18', '1_F_18', '1_G_18', '1_H_18', '1_I_18', '1_J_18', '1_K_18', '1_L_18', '1_M_18', '1_N_18', '1_O_18', '1_P_18',
            '1_E_19', '1_F_19', '1_G_19', '1_H_19', '1_I_19', '1_J_19', '1_K_19', '1_L_19', '1_M_19', '1_N_19', '1_O_19', '1_P_19',
            '1_E_20', '1_F_20', '1_G_20', '1_H_20', '1_I_20', '1_J_20', '1_K_20', '1_L_20', '1_M_20', '1_N_20', '1_O_20', '1_P_20',
            '1_E_21', '1_F_21', '1_G_21', '1_H_21', '1_I_21', '1_J_21', '1_K_21', '1_L_21', '1_M_21', '1_N_21', '1_O_21', '1_P_21',
            '1_E_22', '1_F_22', '1_G_22', '1_H_22', '1_I_22', '1_J_22', '1_K_22', '1_L_22', '1_M_22', '1_N_22', '1_O_22', '1_P_22',
            '1_E_23', '1_F_23', '1_G_23', '1_H_23', '1_I_23', '1_J_23', '1_K_23', '1_L_23', '1_M_23', '1_N_23', '1_O_23', '1_P_23',
            '1_E_24', '1_F_24', '1_G_24', '1_H_24', '1_I_24', '1_J_24', '1_K_24', '1_L_24', '1_M_24', '1_N_24', '1_O_24', '1_P_24',
            '1_E_25', '1_F_25', '1_G_25', '1_H_25', '1_I_25', '1_J_25', '1_K_25', '1_L_25', '1_M_25', '1_N_25', '1_O_25', '1_P_25']

        return [1]

    def format_f(self, num, decimals=2):
        """
        Formats float with comma decimal separator

        :param num:
        :param decimals:
        :return:
        """

        if isinstance(num, (float, int)):
            fstring = '%%.%df' % decimals
            return (fstring % num).replace('.', ',')
        return num

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

                ##################
                # # Posiciones # #
                ##################

                file_path = '/home/paup/Documents/cir2021/B4_v8.txt'
                columns = [str(x) for x in range(25)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                trafo = OrderedDict()

                # # EQUIPADAS CON INTERRUPTOR # #
                df = df[df['6'] == 1]

                # # FINANCIADO 0% # #
                df_f0 = df[df['24'] == 0]

                # TIPO 0
                df_f0_to = df_f0[df_f0['14'] == 0]
                trafo['1_E_18'] = df_f0_to.shape[0]
                trafo['1_F_18'] = ''
                trafo['1_G_18'] = df_f0_to['18'].sum()
                trafo['1_H_18'] = trafo['1_G_18']

                # TIPO 1
                df_f0_t1 = df_f0[df_f0['14'] == 1]
                trafo['1_I_18'] = df_f0_t1.shape[0]
                trafo['1_J_18'] = ''
                trafo['1_K_18'] = df_f0_t1['18'].sum()
                trafo['1_L_18'] = trafo['1_K_18']

                # TOTAL
                trafo['1_M_18'] = trafo['1_E_18'] + trafo['1_I_18']
                trafo['1_N_18'] = trafo['1_F_18'] + trafo['1_J_18']
                trafo['1_O_18'] = trafo['1_G_18'] + trafo['1_K_18']
                trafo['1_P_18'] = trafo['1_O_18']

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['24']) & (df['24'] < 100)]

                # TIPO 0
                df_f_to = df_f[df_f['14'] == 0]
                trafo['2_E_18'] = df_f_to.shape[0]
                trafo['2_F_18'] = ''
                trafo['2_G_18'] = df_f_to['18'].sum()
                trafo['2_H_18'] = trafo['2_G_18']

                # TIPO 1
                df_f_t1 = df_f[df_f['14'] == 1]
                trafo['2_I_18'] = df_f_t1.shape[0]
                trafo['2_J_18'] = ''
                trafo['2_K_18'] = df_f_t1['18'].sum()
                trafo['2_L_18'] = trafo['2_K_18']

                # TOTAL
                trafo['2_M_18'] = trafo['2_E_18'] + trafo['2_I_18']
                trafo['2_N_18'] = trafo['2_F_18'] + trafo['2_J_18']
                trafo['2_O_18'] = trafo['2_G_18'] + trafo['2_K_18']
                trafo['2_P_18'] = trafo['2_O_18']

                # # FINANCIADO 100% # #
                df_f100 = df[df['24'] == 100]

                trafo['3_E_18'] = df_f100.shape[0]
                trafo['3_F_18'] = ''
                trafo['3_G_18'] = df_f100['18'].sum()
                trafo['3_H_18'] = trafo['3_G_18']

                self.output_q.put(['-----', 'TRAFOS', '-----'])
                for k, v in trafo.items():
                    print(k, v)
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ################
                # # Máquinas # #
                ################

                file_path = '/home/paup/Documents/cir2021/B5_v8.txt'
                columns = [str(x) for x in range(25)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                trafo = OrderedDict()

                # # FINANCIADO 0% # #
                df_f0 = df[df['21'] == 0]

                # TIPO 0
                df_f0_to = df_f0[df_f0['12'] == 0]
                trafo['1_E_19'] = df_f0_to.shape[0]
                trafo['1_F_19'] = df_f0_to['5'].sum()
                trafo['1_G_19'] = df_f0_to['20'].sum()
                trafo['1_H_19'] = trafo['1_G_19']

                # TIPO 1
                df_f0_t1 = df_f0[df_f0['12'] == 1]
                trafo['1_I_19'] = df_f0_t1.shape[0]
                trafo['1_J_19'] = df_f0_to['5'].sum()
                trafo['1_K_19'] = df_f0_t1['20'].sum()
                trafo['1_L_19'] = trafo['1_K_19']

                # TOTAL
                trafo['1_M_19'] = trafo['1_E_19'] + trafo['1_I_19']
                trafo['1_N_19'] = trafo['1_F_19'] + trafo['1_J_19']
                trafo['1_O_19'] = trafo['1_G_19'] + trafo['1_K_19']
                trafo['1_P_19'] = trafo['1_O_19']

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['21']) & (df['21'] < 100)]

                # TIPO 0
                df_f_to = df_f[df_f['12'] == 0]
                trafo['2_E_19'] = df_f_to.shape[0]
                trafo['2_F_19'] = df_f0_to['5'].sum()
                trafo['2_G_19'] = df_f_to['20'].sum()
                trafo['2_H_19'] = trafo['2_G_19']

                # TIPO 1
                df_f_t1 = df_f[df_f['12'] == 1]
                trafo['2_I_19'] = df_f_t1.shape[0]
                trafo['2_J_19'] = df_f0_to['5'].sum()
                trafo['2_K_19'] = df_f_t1['20'].sum()
                trafo['2_L_19'] = trafo['2_K_19']

                # TOTAL
                trafo['2_M_19'] = trafo['2_E_19'] + trafo['2_I_19']
                trafo['2_N_19'] = trafo['2_F_19'] + trafo['2_J_19']
                trafo['2_O_19'] = trafo['2_G_19'] + trafo['2_K_19']
                trafo['2_P_19'] = trafo['2_O_19']

                # # FINANCIADO 100% # #
                df_f100 = df[df['21'] == 100]

                trafo['3_E_19'] = df_f100.shape[0]
                trafo['3_F_19'] = df_f0_to['5'].sum()
                trafo['3_G_19'] = df_f100['20'].sum()
                trafo['3_H_19'] = trafo['3_G_19']

                self.output_q.put(['-----', 'TRAFOS', '-----'])
                for k, v in trafo.items():
                    print(k, v)
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ##################
                # # Fiabilidad # #
                ##################

                file_path = '/home/paup/Documents/cir2021/B6_2020_v8.txt'
                columns = [str(x) for x in range(31)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                cel = OrderedDict()

                # # FINANCIADO 0% # #
                df_f0 = df[df['27'] == 0]

                # TIPO 0
                df_f0_to = df_f0[df_f0['19'] == 0]
                cel['1_E_20'] = df_f0_to.shape[0]
                cel['1_F_20'] = ''
                cel['1_G_20'] = df_f0_to['26'].sum()
                cel['1_H_20'] = cel['1_G_20']

                # TIPO 1
                df_f0_t1 = df_f0[df_f0['19'] == 1]
                cel['1_I_20'] = df_f0_t1.shape[0]
                cel['1_J_20'] = ''
                cel['1_K_20'] = df_f0_t1['26'].sum()
                cel['1_L_20'] = cel['1_K_20']

                # TOTAL
                cel['1_M_20'] = cel['1_E_20'] + cel['1_I_20']
                cel['1_N_20'] = cel['1_F_20'] + cel['1_J_20']
                cel['1_O_20'] = cel['1_G_20'] + cel['1_K_20']
                cel['1_P_20'] = cel['1_O_20']

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['27']) & (df['27'] < 100)]

                # TIPO 0
                df_f_to = df_f[df_f['19'] == 0]
                cel['2_E_20'] = df_f_to.shape[0]
                cel['2_F_20'] = ''
                cel['2_G_20'] = df_f_to['26'].sum()
                cel['2_H_20'] = cel['2_G_20']

                # TIPO 1
                df_f_t1 = df_f[df_f['19'] == 1]
                cel['2_I_20'] = df_f_t1.shape[0]
                cel['2_J_20'] = ''
                cel['2_K_20'] = df_f_t1['26'].sum()
                cel['2_L_20'] = cel['2_K_20']

                # TOTAL
                cel['2_M_20'] = cel['2_E_20'] + cel['2_I_20']
                cel['2_N_20'] = cel['2_F_20'] + cel['2_J_20']
                cel['2_O_20'] = cel['2_G_20'] + cel['2_K_20']
                cel['2_P_20'] = cel['2_O_20']

                # # FINANCIADO 100% # #
                df_f100 = df[df['27'] == 100]

                cel['3_E_20'] = df_f100.shape[0]
                cel['3_F_20'] = ''
                cel['3_G_20'] = df_f100['26'].sum()
                cel['3_H_20'] = cel['3_G_20']

                self.output_q.put(['-----', 'FIABILIDAD', '-----'])
                for k, v in cel.items():
                    print(k, v)
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ###########
                # # CTs # #
                ###########

                file_path = '/home/paup/Documents/cir2021/B2_v8.txt'
                columns = [str(x) for x in range(36)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                ct = OrderedDict()

                # # FINANCIADO 0% # #
                df_f0 = df[df['31'] == 0]

                # TIPO 0
                df_f0_to = df_f0[df_f0['23'] == 0]
                ct['1_E_21'] = df_f0_to.shape[0]
                ct['1_F_21'] = df_f0_to['8'].sum() / 1000
                ct['1_G_21'] = df_f0_to['30'].sum()
                ct['1_H_21'] = ct['1_G_21']

                # TIPO 1
                df_f0_t1 = df_f0[df_f0['23'] == 1]
                ct['1_I_21'] = df_f0_t1.shape[0]
                ct['1_J_21'] = df_f0_t1['8'].sum() / 1000
                ct['1_K_21'] = df_f0_t1['30'].sum()
                ct['1_L_21'] = ct['1_K_21']

                # TOTAL
                ct['1_M_21'] = ct['1_E_21'] + ct['1_I_21']
                ct['1_N_21'] = ct['1_F_21'] + ct['1_J_21']
                ct['1_O_21'] = ct['1_G_21'] + ct['1_K_21']
                ct['1_P_21'] = ct['1_O_21']

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['31']) & (df['31'] < 100)]

                # TIPO 0
                df_f_to = df_f[df_f['23'] == 0]
                ct['2_E_21'] = df_f_to.shape[0]
                ct['2_F_21'] = df_f_to['8'].sum() / 1000
                ct['2_G_21'] = df_f_to['30'].sum()
                ct['2_H_21'] = ct['2_G_21']

                # TIPO 1
                df_f_t1 = df_f[df_f['23'] == 1]
                ct['2_I_21'] = df_f_t1.shape[0]
                ct['2_J_21'] = df_f_t1['8'].sum() / 1000
                ct['2_K_21'] = df_f_t1['30'].sum()
                ct['2_L_21'] = ct['2_K_21']

                # TOTAL
                ct['2_M_21'] = ct['2_E_21'] + ct['2_I_21']
                ct['2_N_21'] = ct['2_F_21'] + ct['2_J_21']
                ct['2_O_21'] = ct['2_G_21'] + ct['2_K_21']
                ct['2_P_21'] = ct['2_O_21']

                # # FINANCIADO 100% # #
                df_f100 = df[df['31'] == 100]

                ct['3_E_21'] = df_f100.shape[0]
                ct['3_F_21'] = df_f100['8'].sum() / 1000
                ct['3_G_21'] = df_f100['30'].sum()
                ct['3_H_21'] = ct['3_G_21']

                self.output_q.put(['-----', 'CTs', '-----'])
                for k, v in ct.items():
                    print(k, v)
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
