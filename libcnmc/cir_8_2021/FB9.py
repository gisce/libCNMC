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

                ##########
                # # AT # #
                ##########

                file_path = '/home/paup/Documents/cir2021/B1_v8.txt'
                columns = [str(x) for x in range(35)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                trafo = OrderedDict()

                # # AT # #
                df = df[df['8'] >= 1]

                # # FINANCIADO 0% # #
                df_f0 = df[df['28'] == 0]

                # TIPO 0
                df_f0_to = df_f0[df_f0['22'] == 0]
                trafo['1_E_16'] = df_f0_to.shape[0]
                trafo['1_F_16'] = df_f0_to['10'].sum()
                trafo['1_G_16'] = df_f0_to['27'].sum()
                trafo['1_H_16'] = trafo['1_G_16']

                # TIPO 1
                df_f0_t1 = df_f0[df_f0['22'] == 1]
                trafo['1_I_16'] = df_f0_t1.shape[0]
                trafo['1_J_16'] = df_f0_t1['10'].sum()
                trafo['1_K_16'] = df_f0_t1['27'].sum()
                trafo['1_L_16'] = trafo['1_K_16']

                # TOTAL
                trafo['1_M_16'] = trafo['1_E_16'] + trafo['1_I_16']
                trafo['1_N_16'] = trafo['1_F_16'] + trafo['1_J_16']
                trafo['1_O_16'] = trafo['1_G_16'] + trafo['1_K_16']
                trafo['1_P_16'] = trafo['1_O_16']

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['28']) & (df['28'] < 100)]

                # TIPO 0
                df_f_to = df_f[df_f['22'] == 0]
                trafo['2_E_16'] = df_f_to.shape[0]
                trafo['2_F_16'] = df_f_to['10'].sum()
                trafo['2_G_16'] = df_f_to['27'].sum()
                trafo['2_H_16'] = trafo['2_G_16']

                # TIPO 1
                df_f_t1 = df_f[df_f['22'] == 1]
                trafo['2_I_16'] = df_f_t1.shape[0]
                trafo['2_J_16'] = df_f_t1['10'].sum()
                trafo['2_K_16'] = df_f_t1['27'].sum()
                trafo['2_L_16'] = trafo['2_K_16']

                # TOTAL
                trafo['2_M_16'] = trafo['2_E_16'] + trafo['2_I_16']
                trafo['2_N_16'] = trafo['2_F_16'] + trafo['2_J_16']
                trafo['2_O_16'] = trafo['2_G_16'] + trafo['2_K_16']
                trafo['2_P_16'] = trafo['2_O_16']

                # # FINANCIADO 100% # #
                df_f100 = df[df['28'] == 100]

                trafo['3_E_16'] = df_f100.shape[0]
                trafo['3_F_16'] = df_f100['10'].sum()
                trafo['3_G_16'] = df_f100['27'].sum()
                trafo['3_H_16'] = trafo['3_G_16']

                self.output_q.put(['-----', 'AT', '-----'])
                for k, v in trafo.items():
                    print(k, v)
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ##########
                # # BT # #
                ##########

                file_path = '/home/paup/Documents/cir2021/B1_v8.txt'
                columns = [str(x) for x in range(35)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                trafo = OrderedDict()

                # # BT # #
                df = df[df['8'] < 1]

                # # FINANCIADO 0% # #
                df_f0 = df[df['28'] == 0]

                # TIPO 0
                df_f0_to = df_f0[df_f0['22'] == 0]
                trafo['1_E_17'] = df_f0_to.shape[0]
                trafo['1_F_17'] = df_f0_to['10'].sum()
                trafo['1_G_17'] = df_f0_to['27'].sum()
                trafo['1_H_17'] = trafo['1_G_17']

                # TIPO 1
                df_f0_t1 = df_f0[df_f0['22'] == 1]
                trafo['1_I_17'] = df_f0_t1.shape[0]
                trafo['1_J_17'] = df_f0_t1['10'].sum()
                trafo['1_K_17'] = df_f0_t1['27'].sum()
                trafo['1_L_17'] = trafo['1_K_17']

                # TOTAL
                trafo['1_M_17'] = trafo['1_E_17'] + trafo['1_I_17']
                trafo['1_N_17'] = trafo['1_F_17'] + trafo['1_J_17']
                trafo['1_O_17'] = trafo['1_G_17'] + trafo['1_K_17']
                trafo['1_P_17'] = trafo['1_O_17']

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['28']) & (df['28'] < 100)]

                # TIPO 0
                df_f_to = df_f[df_f['22'] == 0]
                trafo['2_E_17'] = df_f_to.shape[0]
                trafo['2_F_17'] = df_f_to['10'].sum()
                trafo['2_G_17'] = df_f_to['27'].sum()
                trafo['2_H_17'] = trafo['2_G_17']

                # TIPO 1
                df_f_t1 = df_f[df_f['22'] == 1]
                trafo['2_I_17'] = df_f_t1.shape[0]
                trafo['2_J_17'] = df_f_t1['10'].sum()
                trafo['2_K_17'] = df_f_t1['27'].sum()
                trafo['2_L_17'] = trafo['2_K_17']

                # TOTAL
                trafo['2_M_17'] = trafo['2_E_17'] + trafo['2_I_17']
                trafo['2_N_17'] = trafo['2_F_17'] + trafo['2_J_17']
                trafo['2_O_17'] = trafo['2_G_17'] + trafo['2_K_17']
                trafo['2_P_17'] = trafo['2_O_17']

                # # FINANCIADO 100% # #
                df_f100 = df[df['28'] == 100]

                trafo['3_E_17'] = df_f100.shape[0]
                trafo['3_F_17'] = df_f100['10'].sum()
                trafo['3_G_17'] = df_f100['27'].sum()
                trafo['3_H_17'] = trafo['3_G_17']

                self.output_q.put(['-----', 'BT', '-----'])
                for k, v in trafo.items():
                    print(k, v)
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

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

                self.output_q.put(['-----', 'POSICIONES', '-----'])
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
                trafo['1_J_19'] = df_f0_t1['5'].sum()
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
                trafo['2_F_19'] = df_f_to['5'].sum()
                trafo['2_G_19'] = df_f_to['20'].sum()
                trafo['2_H_19'] = trafo['2_G_19']

                # TIPO 1
                df_f_t1 = df_f[df_f['12'] == 1]
                trafo['2_I_19'] = df_f_t1.shape[0]
                trafo['2_J_19'] = df_f_t1['5'].sum()
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
                trafo['3_F_19'] = df_f100['5'].sum()
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

                ############################
                # # TOTAL INVERSIONES UF # #
                ############################

                inv_uf = OrderedDict()

                # # FINANCIADO 0% # #

                # TIPO 0
                inv_uf['1_E_22'] = ['1_E_16'] + ['1_E_17'] + ['1_E_18'] + ['1_E_19'] + ['1_E_20'] + ['1_E_21']
                inv_uf['1_F_22'] = ['1_F_16'] + ['1_F_17'] + ['1_F_18'] + ['1_F_19'] + ['1_F_20'] + ['1_F_21']
                inv_uf['1_G_22'] = ['1_G_16'] + ['1_G_17'] + ['1_G_18'] + ['1_G_19'] + ['1_G_20'] + ['1_G_21']
                inv_uf['1_H_22'] = ['1_H_16'] + ['1_H_17'] + ['1_H_18'] + ['1_H_19'] + ['1_H_20'] + ['1_H_21']

                # TIPO 1
                inv_uf['1_I_22'] = ['1_I_16'] + ['1_I_17'] + ['1_I_18'] + ['1_I_19'] + ['1_I_20'] + ['1_I_21']
                inv_uf['1_J_22'] = ['1_J_16'] + ['1_J_17'] + ['1_J_18'] + ['1_J_19'] + ['1_J_20'] + ['1_J_21']
                inv_uf['1_K_22'] = ['1_K_16'] + ['1_K_17'] + ['1_K_18'] + ['1_K_19'] + ['1_K_20'] + ['1_K_21']
                inv_uf['1_L_22'] = ['1_L_16'] + ['1_L_17'] + ['1_L_18'] + ['1_L_19'] + ['1_L_20'] + ['1_L_21']

                # TOTAL
                inv_uf['1_M_22'] = ['1_M_16'] + ['1_M_17'] + ['1_M_18'] + ['1_M_19'] + ['1_M_20'] + ['1_M_21']
                inv_uf['1_N_22'] = ['1_N_16'] + ['1_N_17'] + ['1_N_18'] + ['1_N_19'] + ['1_N_20'] + ['1_N_21']
                inv_uf['1_O_22'] = ['1_O_16'] + ['1_O_17'] + ['1_O_18'] + ['1_O_19'] + ['1_O_20'] + ['1_O_21']
                inv_uf['1_P_22'] = ['1_P_16'] + ['1_P_17'] + ['1_P_18'] + ['1_P_19'] + ['1_P_20'] + ['1_P_21']

                # # 0% < FINANCIADO < 100% # #

                # TIPO 0
                inv_uf['2_E_22'] = ['2_E_16'] + ['2_E_17'] + ['2_E_18'] + ['2_E_19'] + ['2_E_20'] + ['2_E_21']
                inv_uf['2_F_22'] = ['2_F_16'] + ['2_F_17'] + ['2_F_18'] + ['2_F_19'] + ['2_F_20'] + ['2_F_21']
                inv_uf['2_G_22'] = ['2_G_16'] + ['2_G_17'] + ['2_G_18'] + ['2_G_19'] + ['2_G_20'] + ['2_G_21']
                inv_uf['2_H_22'] = ['2_H_16'] + ['2_H_17'] + ['2_H_18'] + ['2_H_19'] + ['2_H_20'] + ['2_H_21']

                # TIPO 1
                inv_uf['2_I_22'] = ['2_I_16'] + ['2_I_17'] + ['2_I_18'] + ['2_I_19'] + ['2_I_20'] + ['2_I_21']
                inv_uf['2_J_22'] = ['2_J_16'] + ['2_J_17'] + ['2_J_18'] + ['2_J_19'] + ['2_J_20'] + ['2_J_21']
                inv_uf['2_K_22'] = ['2_K_16'] + ['2_K_17'] + ['2_K_18'] + ['2_K_19'] + ['2_K_20'] + ['2_K_21']
                inv_uf['2_L_22'] = ['2_L_16'] + ['2_L_17'] + ['2_L_18'] + ['2_L_19'] + ['2_L_20'] + ['2_L_21']

                # TOTAL
                inv_uf['2_M_22'] = ['2_M_16'] + ['2_M_17'] + ['2_M_18'] + ['2_M_19'] + ['2_M_20'] + ['2_M_21']
                inv_uf['2_N_22'] = ['2_N_16'] + ['2_N_17'] + ['2_N_18'] + ['2_N_19'] + ['2_N_20'] + ['2_N_21']
                inv_uf['2_O_22'] = ['2_O_16'] + ['2_O_17'] + ['2_O_18'] + ['2_O_19'] + ['2_O_20'] + ['2_O_21']
                inv_uf['2_P_22'] = ['2_P_16'] + ['2_P_17'] + ['2_P_18'] + ['2_P_19'] + ['2_P_20'] + ['2_P_21']

                # # FINANCIADO 100% # #
                inv_uf['3_E_22'] = ['3_E_16'] + ['3_E_17'] + ['3_E_18'] + ['3_E_19'] + ['3_E_20'] + ['3_E_21']
                inv_uf['3_F_22'] = ['3_F_16'] + ['3_F_17'] + ['3_F_18'] + ['3_F_19'] + ['3_F_20'] + ['3_F_21']
                inv_uf['3_G_22'] = ['3_G_16'] + ['3_G_17'] + ['3_G_18'] + ['3_G_19'] + ['3_G_20'] + ['3_G_21']
                inv_uf['3_H_22'] = ['3_H_16'] + ['3_H_17'] + ['3_H_18'] + ['3_H_19'] + ['3_H_20'] + ['3_H_21']

                ###########################
                # # OTROS INMOVILIZADOS # #
                ###########################

                file_path = '/home/paup/Documents/cir2021/B8_v8.txt'
                columns = [str(x) for x in range(17)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                desp = OrderedDict()

                # # FINANCIADO 0% # #
                df_f0 = df[df['15'] == 0]

                # NO DIGITALIZACION #
                df_f0_des = df_f0[df_f0['2'] == 'DES']
                desp['1B_D_13'] = df_f0_des.shape[0]
                desp['1B_E_13'] = df_f0_des['14'].sum()
                desp['1B_F_13'] = df_f0_des['1B_E_13']

                # TERRENOS #
                df_f0_ter = df_f0[df_f0['2'] == 'TER']
                desp['1B_D_14'] = df_f0_ter.shape[0]
                desp['1B_E_14'] = df_f0_ter['14'].sum()
                desp['1B_F_14'] = df_f0_ter['1B_E_14']

                # DIGITALIZACION #
                df_f0_dig = df_f0[df_f0['2'] == 'DIG']
                desp['1B_D_15'] = df_f0_dig.shape[0]
                desp['1B_E_15'] = df_f0_dig['14'].sum()
                desp['1B_F_15'] = df_f0_dig['1B_E_15']

                # OTRO IBO #
                df_f0_ibo = df_f0[df_f0['2'] == 'IBO']
                desp['1B_D_16'] = df_f0_ibo.shape[0]
                desp['1B_E_16'] = df_f0_ibo['14'].sum()
                desp['1B_F_16'] = df_f0_ibo['1B_E_16']

                # TOTAL #
                desp['1B_D_17'] = desp['1B_D_13'] + desp['1B_D_14'] + desp['1B_D_15'] + desp['1B_D_16']
                desp['1B_E_17'] = desp['1B_E_13'] + desp['1B_E_14'] + desp['1B_E_15'] + desp['1B_E_16']
                desp['1B_F_17'] = desp['1B_F_13'] + desp['1B_F_14'] + desp['1B_F_15'] + desp['1B_F_16']

                desp['1_E_24'] = desp['1B_D_17']
                desp['1_F_24'] = ''
                desp['1_G_24'] = desp['1B_E_17']
                desp['1_H_24'] = desp['1B_F_17']

                desp['1_I_24'] = ''
                desp['1_J_24'] = ''
                desp['1_K_24'] = ''
                desp['1_L_24'] = ''

                desp['1_M_24'] = desp['1_E_24']
                desp['1_N_24'] = desp['1_F_24']
                desp['1_O_24'] = desp['1_G_24']
                desp['1_P_24'] = desp['1_H_24']

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['15']) & (df['15'] < 100)]

                # NO DIGITALIZACION #
                df_f_des = df_f[df_f['2'] == 'DES']
                desp['2A_D_13'] = df_f_des.shape[0]
                desp['2A_E_13'] = df_f_des['14'].sum()
                desp['2A_F_13'] = df_f_des['2A_E_13']

                # TERRENOS #
                df_f_ter = df_f[df_f['2'] == 'TER']
                desp['2A_D_14'] = df_f_ter.shape[0]
                desp['2A_E_14'] = df_f_ter['14'].sum()
                desp['2A_F_14'] = df_f_ter['2A_E_14']

                # DIGITALIZACION #
                df_f_dig = df_f[df_f['2'] == 'DIG']
                desp['2A_D_15'] = df_f_dig.shape[0]
                desp['2A_E_15'] = df_f_dig['14'].sum()
                desp['2A_F_15'] = df_f_dig['2A_E_15']

                # OTRO IBO #
                df_f_ibo = df_f[df_f['2'] == 'IBO']
                desp['2A_D_16'] = df_f_ibo.shape[0]
                desp['2A_E_16'] = df_f_ibo['14'].sum()
                desp['2A_F_16'] = df_f_ibo['2A_E_16']

                # TOTAL #
                desp['2A_D_17'] = desp['2A_D_13'] + desp['2A_D_14'] + desp['2A_D_15'] + desp['2A_D_16']
                desp['2A_E_17'] = desp['2A_E_13'] + desp['2A_E_14'] + desp['2A_E_15'] + desp['2A_E_16']
                desp['2A_F_17'] = desp['2A_F_13'] + desp['2A_F_14'] + desp['2A_F_15'] + desp['2A_F_16']

                desp['2_E_24'] = desp['2A_D_17']
                desp['2_F_24'] = ''
                desp['2_G_24'] = desp['2A_E_17']
                desp['2_H_24'] = desp['2A_F_17']

                desp['2_I_24'] = ''
                desp['2_J_24'] = ''
                desp['2_K_24'] = ''
                desp['2_L_24'] = ''

                desp['2_M_24'] = desp['2_E_24']
                desp['2_N_24'] = desp['2_F_24']
                desp['2_O_24'] = desp['2_G_24']
                desp['2_P_24'] = desp['2_H_24']

                # # FINANCIADO 100% # #
                df_f100 = df[df['15'] == 100]

                desp['3_E_24'] = df_f100.shape[0]
                desp['3_F_24'] = ''
                desp['3_G_24'] = df_f100['14'].sum()
                desp['3_H_24'] = desp['3_G_24']

                self.output_q.put(['-----', 'OTROS INMOVILIZADOS', '-----'])
                for k, v in ct.items():
                    print(k, v)
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                #########################
                # # TOTAL INVERSIONES # #
                #########################

                inv = OrderedDict()

                # # FINANCIADO 0% # #

                # TIPO 0
                inv['1_E_25'] = ['1_E_22'] + ['1_E_23'] + ['1_E_24']
                inv['1_F_25'] = ['1_F_22'] + ['1_F_23'] + ['1_F_24']
                inv['1_G_25'] = ['1_G_22'] + ['1_G_23'] + ['1_G_24']
                inv['1_H_25'] = ['1_H_22'] + ['1_H_23'] + ['1_H_24']

                # TIPO 1
                inv['1_I_25'] = ['1_I_22'] + ['1_I_23'] + ['1_I_24']
                inv['1_J_25'] = ['1_J_22'] + ['1_J_23'] + ['1_J_24']
                inv['1_K_25'] = ['1_K_22'] + ['1_K_23'] + ['1_K_24']
                inv['1_L_25'] = ['1_L_22'] + ['1_L_23'] + ['1_L_24']

                # TOTAL
                inv['1_M_25'] = ['1_M_22'] + ['1_M_23'] + ['1_M_24']
                inv['1_N_25'] = ['1_N_22'] + ['1_N_23'] + ['1_N_24']
                inv['1_O_25'] = ['1_O_22'] + ['1_O_23'] + ['1_O_24']
                inv['1_P_25'] = ['1_P_22'] + ['1_P_23'] + ['1_P_24']

                # # 0% < FINANCIADO < 100% # #

                # TIPO 0
                inv['2_E_25'] = ['2_E_22'] + ['2_E_23'] + ['2_E_24']
                inv['2_F_25'] = ['2_F_22'] + ['2_F_23'] + ['2_F_24']
                inv['2_G_25'] = ['2_G_22'] + ['2_G_23'] + ['2_G_24']
                inv['2_H_25'] = ['2_H_22'] + ['2_H_23'] + ['2_H_24']

                # TIPO 1
                inv['2_I_25'] = ['2_I_22'] + ['2_I_23'] + ['2_I_24']
                inv['2_J_25'] = ['2_J_22'] + ['2_J_23'] + ['2_J_24']
                inv['2_K_25'] = ['2_K_22'] + ['2_K_23'] + ['2_K_24']
                inv['2_L_25'] = ['2_L_22'] + ['2_L_23'] + ['2_L_24']

                # TOTAL
                inv['2_M_25'] = ['2_M_22'] + ['2_M_23'] + ['2_M_24']
                inv['2_N_25'] = ['2_N_22'] + ['2_N_23'] + ['2_N_24']
                inv['2_O_25'] = ['2_O_22'] + ['2_O_23'] + ['2_O_24']
                inv['2_P_25'] = ['2_P_22'] + ['2_P_23'] + ['2_P_24']

                # # FINANCIADO 100% # #
                inv['3_E_25'] = ['3_E_22'] + ['3_E_23'] + ['3_E_24']
                inv['3_F_25'] = ['3_F_22'] + ['3_F_23'] + ['3_F_24']
                inv['3_G_25'] = ['3_G_22'] + ['3_G_23'] + ['3_G_24']
                inv['3_H_25'] = ['3_H_22'] + ['3_H_23'] + ['3_H_24']

                self.output_q.put(['-----', 'TOTAL INVERSIONES', '-----'])
                for k, v in inv.items():
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
