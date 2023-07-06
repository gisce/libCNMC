#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import StopMultiprocessBased
import pandas as pd
from collections import OrderedDict


class FB9(StopMultiprocessBased):

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
        self.prefix_AT = kwargs.pop('prefix_at', 'A') or 'A'
        self.prefix_BT = kwargs.pop('prefix_bt', 'B') or 'B'

    def get_sequence(self):

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

        while True:
            try:
                # generar linies
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                # RESUMEN

                resumen = OrderedDict()
                resumen['5_G_6'] = 0
                resumen['5_G_7'] = 0
                resumen['5_G_8'] = 0

                at_obj = O.GiscedataAtTram
                bt_obj = O.GiscedataBtElement
                pos_obj = O.GiscedataCtsSubestacionsPosicio
                cella_obj = O.GiscedataCellesCella
                ct_obj = O.GiscedataCts
                models = [at_obj, bt_obj, pos_obj, cella_obj, ct_obj]

                names = {}
                for model in models:
                    model_ids = model.search([('cedida', '=', True)])
                    names_data = model.read(model_ids, ['name'])

                    names[model._name] = []
                    for name_data in names_data:
                        names[model._name].append(name_data['name'])

                print(names)

                ##########
                # # AT # #
                ##########

                file_path = '/tmp/8_2021_loaded_or_generated_b1.txt'
                columns = [str(x) for x in range(35)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns,
                                 dtype={'0': 'object', '21': 'object', '23': 'object', '34': 'object'})
                df['28'] = pd.to_numeric(df['28'], errors='coerce').astype(float)
                df['22'] = pd.to_numeric(df['22'], errors='coerce').astype(float)
                at = OrderedDict()

                # # AT # #
                df = df[df['8'] >= 1]

                # CUADRO 5
                resumen['5_G_6'] += df['29'].sum()
                resumen['5_G_7'] += df['31'].sum()
                resumen['5_G_8'] += df['30'].sum()

                names_at_prefix = []
                for name in names['giscedata.at.tram']:
                    names_at_prefix.append('{}{}'.format(self.prefix_AT, name))
                df_5_g_10 = df[df['0'].isin(names_at_prefix)]
                resumen['5_G_9'] = df_5_g_10['27'].sum()

                # # FINANCIADO 0% # #
                df_f0 = df[df['28'] == 0]

                # TIPO 0
                df_f0_to = df_f0[df_f0['22'] == 0]
                at['1_E_16'] = df_f0_to.shape[0]
                at['1_F_16'] = df_f0_to['10'].sum()
                at['1_G_16'] = df_f0_to['27'].sum()
                at['1_H_16'] = at['1_G_16']

                # TIPO 1
                df_f0_t1 = df_f0[df_f0['22'] == 1]
                at['1_I_16'] = df_f0_t1.shape[0]
                at['1_J_16'] = df_f0_t1['10'].sum()
                at['1_K_16'] = df_f0_t1['27'].sum()
                at['1_L_16'] = at['1_K_16']

                # TOTAL
                at['1_M_16'] = at['1_E_16'] + at['1_I_16']
                at['1_N_16'] = at['1_F_16'] + at['1_J_16']
                at['1_O_16'] = at['1_G_16'] + at['1_K_16']
                at['1_P_16'] = at['1_O_16']

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['28']) & (df['28'] < 100)]

                # TIPO 0
                df_f_to = df_f[df_f['22'] == 0]
                at['2_E_16'] = df_f_to.shape[0]
                at['2_F_16'] = df_f_to['10'].sum()
                at['2_G_16'] = df_f_to['27'].sum()
                at['2_H_16'] = at['2_G_16']

                # TIPO 1
                df_f_t1 = df_f[df_f['22'] == 1]
                at['2_I_16'] = df_f_t1.shape[0]
                at['2_J_16'] = df_f_t1['10'].sum()
                at['2_K_16'] = df_f_t1['27'].sum()
                at['2_L_16'] = at['2_K_16']

                # TOTAL
                at['2_M_16'] = at['2_E_16'] + at['2_I_16']
                at['2_N_16'] = at['2_F_16'] + at['2_J_16']
                at['2_O_16'] = at['2_G_16'] + at['2_K_16']
                at['2_P_16'] = at['2_O_16']

                # # FINANCIADO 100% # #
                df_f100 = df[(df['28'] == 100) & ((df['22'] == 0) | (df['22'] == 1))]

                at['3_E_16'] = df_f100.shape[0]
                at['3_F_16'] = df_f100['10'].sum()
                at['3_G_16'] = df_f100['27'].sum()
                at['3_H_16'] = at['3_G_16']

                for k, v in at.items():
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ##########
                # # BT # #
                ##########

                file_path = '/tmp/8_2021_loaded_or_generated_b1.txt'
                columns = [str(x) for x in range(35)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns,
                                 dtype={'0': 'object', '21': 'object', '23': 'object', '34': 'object'})
                df['28'] = pd.to_numeric(df['28'], errors='coerce').astype(float)
                df['22'] = pd.to_numeric(df['22'], errors='coerce').astype(float)
                bt = OrderedDict()

                # # BT # #
                df = df[df['8'] < 1]

                # CUADRO 5
                resumen['5_G_6'] += df['29'].sum()
                resumen['5_G_7'] += df['31'].sum()
                resumen['5_G_8'] += df['30'].sum()

                names_bt_prefix = []
                for name in names['giscedata.bt.element']:
                    names_bt_prefix.append('{}{}'.format(self.prefix_BT, name))
                df_5_g_10 = df[df['0'].isin(names_bt_prefix)]
                resumen['5_G_9'] += df_5_g_10['27'].sum()

                # # FINANCIADO 0% # #
                df_f0 = df[df['28'] == 0]

                # TIPO 0
                df_f0_to = df_f0[df_f0['22'] == 0]
                bt['1_E_17'] = df_f0_to.shape[0]
                bt['1_F_17'] = df_f0_to['10'].sum()
                bt['1_G_17'] = df_f0_to['27'].sum()
                bt['1_H_17'] = bt['1_G_17']

                # TIPO 1
                df_f0_t1 = df_f0[df_f0['22'] == 1]
                bt['1_I_17'] = df_f0_t1.shape[0]
                bt['1_J_17'] = df_f0_t1['10'].sum()
                bt['1_K_17'] = df_f0_t1['27'].sum()
                bt['1_L_17'] = bt['1_K_17']

                # TOTAL
                bt['1_M_17'] = bt['1_E_17'] + bt['1_I_17']
                bt['1_N_17'] = bt['1_F_17'] + bt['1_J_17']
                bt['1_O_17'] = bt['1_G_17'] + bt['1_K_17']
                bt['1_P_17'] = bt['1_O_17']

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['28']) & (df['28'] < 100)]

                # TIPO 0
                df_f_to = df_f[df_f['22'] == 0]
                bt['2_E_17'] = df_f_to.shape[0]
                bt['2_F_17'] = df_f_to['10'].sum()
                bt['2_G_17'] = df_f_to['27'].sum()
                bt['2_H_17'] = bt['2_G_17']

                # TIPO 1
                df_f_t1 = df_f[df_f['22'] == 1]
                bt['2_I_17'] = df_f_t1.shape[0]
                bt['2_J_17'] = df_f_t1['10'].sum()
                bt['2_K_17'] = df_f_t1['27'].sum()
                bt['2_L_17'] = bt['2_K_17']

                # TOTAL
                bt['2_M_17'] = bt['2_E_17'] + bt['2_I_17']
                bt['2_N_17'] = bt['2_F_17'] + bt['2_J_17']
                bt['2_O_17'] = bt['2_G_17'] + bt['2_K_17']
                bt['2_P_17'] = bt['2_O_17']

                # # FINANCIADO 100% # #
                df_f100 = df[(df['28'] == 100) & ((df['22'] == 0) | (df['22'] == 1))]

                bt['3_E_17'] = df_f100.shape[0]
                bt['3_F_17'] = df_f100['10'].sum()
                bt['3_G_17'] = df_f100['27'].sum()
                bt['3_H_17'] = bt['3_G_17']

                for k, v in bt.items():
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ##################
                # # Posiciones # #
                ##################

                file_path = '/tmp/8_2021_loaded_or_generated_b4.txt'
                columns = [str(x) for x in range(27)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                df['24'] = pd.to_numeric(df['24'], errors='coerce').astype(float)
                df['14'] = pd.to_numeric(df['14'], errors='coerce').astype(float)
                pos = OrderedDict()

                # # EQUIPADAS CON INTERRUPTOR # #
                df = df[df['6'].isin([1, 2])]

                # CUADRO 5
                resumen['5_G_6'] += df['20'].sum()
                resumen['5_G_7'] += df['22'].sum()
                resumen['5_G_8'] += df['21'].sum()

                pos_names = names['giscedata.cts.subestacions.posicio']
                df_5_g_10 = df[df['0'].isin(pos_names)]
                resumen['5_G_9'] += df_5_g_10['18'].sum()

                # # FINANCIADO 0% # #
                df_f0 = df[df['24'] == 0]

                # TIPO 0
                df_f0_to = df_f0[df_f0['14'] == 0]
                pos['1_E_18'] = df_f0_to.shape[0]
                pos['1_F_18'] = 0
                pos['1_G_18'] = df_f0_to['18'].sum()
                pos['1_H_18'] = pos['1_G_18']

                # TIPO 1
                df_f0_t1 = df_f0[df_f0['14'] == 1]
                pos['1_I_18'] = df_f0_t1.shape[0]
                pos['1_J_18'] = 0
                pos['1_K_18'] = df_f0_t1['18'].sum()
                pos['1_L_18'] = pos['1_K_18']

                # TOTAL
                pos['1_M_18'] = pos['1_E_18'] + pos['1_I_18']
                pos['1_N_18'] = pos['1_F_18'] + pos['1_J_18']
                pos['1_O_18'] = pos['1_G_18'] + pos['1_K_18']
                pos['1_P_18'] = pos['1_O_18']

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['24']) & (df['24'] < 100)]

                # TIPO 0
                df_f_to = df_f[df_f['14'] == 0]
                pos['2_E_18'] = df_f_to.shape[0]
                pos['2_F_18'] = 0
                pos['2_G_18'] = df_f_to['18'].sum()
                pos['2_H_18'] = pos['2_G_18']

                # TIPO 1
                df_f_t1 = df_f[df_f['14'] == 1]
                pos['2_I_18'] = df_f_t1.shape[0]
                pos['2_J_18'] = 0
                pos['2_K_18'] = df_f_t1['18'].sum()
                pos['2_L_18'] = pos['2_K_18']

                # TOTAL
                pos['2_M_18'] = pos['2_E_18'] + pos['2_I_18']
                pos['2_N_18'] = pos['2_F_18'] + pos['2_J_18']
                pos['2_O_18'] = pos['2_G_18'] + pos['2_K_18']
                pos['2_P_18'] = pos['2_O_18']

                # # FINANCIADO 100% # #
                df_f100 = df[(df['24'] == 100) & ((df['14'] == 0) | (df['14'] == 1))]

                pos['3_E_18'] = df_f100.shape[0]
                pos['3_F_18'] = 0
                pos['3_G_18'] = df_f100['18'].sum()
                pos['3_H_18'] = pos['3_G_18']

                for k, v in pos.items():
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ################
                # # Máquinas # #
                ################

                file_path = '/tmp/8_2021_loaded_or_generated_b5.txt'
                columns = [str(x) for x in range(25)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                df['21'] = pd.to_numeric(df['21'], errors='coerce').astype(float)
                df['12'] = pd.to_numeric(df['12'], errors='coerce').astype(float)
                trafo = OrderedDict()

                resumen['5_G_6'] += df['17'].sum()
                resumen['5_G_7'] += df['19'].sum()
                resumen['5_G_8'] += df['18'].sum()

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
                df_f100 = df[(df['21'] == 100) & ((df['12'] == 0) | (df['12'] == 1))]

                trafo['3_E_19'] = df_f100.shape[0]
                trafo['3_F_19'] = df_f100['5'].sum()
                trafo['3_G_19'] = df_f100['20'].sum()
                trafo['3_H_19'] = trafo['3_G_19']

                for k, v in trafo.items():
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ##################
                # # Fiabilidad # #
                ##################

                file_path = '/tmp/8_2021_loaded_or_generated_b6.txt'
                columns = [str(x) for x in range(31)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                df['27'] = pd.to_numeric(df['27'], errors='coerce').astype(float)
                df['19'] = pd.to_numeric(df['19'], errors='coerce').astype(float)
                cel = OrderedDict()

                # CUADRO 5
                resumen['5_G_6'] += df['23'].sum()
                resumen['5_G_7'] += df['25'].sum()
                resumen['5_G_8'] += df['24'].sum()

                cella_names = names['giscedata.celles.cella']
                df_5_g_10 = df[df['0'].isin(cella_names)]
                resumen['5_G_9'] += df_5_g_10['26'].sum()

                # # FINANCIADO 0% # #
                df_f0 = df[df['27'] == 0]

                # TIPO 0
                df_f0_to = df_f0[df_f0['19'] == 0]
                cel['1_E_20'] = df_f0_to.shape[0]
                cel['1_F_20'] = 0
                cel['1_G_20'] = df_f0_to['26'].sum()
                cel['1_H_20'] = cel['1_G_20']

                # TIPO 1
                df_f0_t1 = df_f0[df_f0['19'] == 1]
                cel['1_I_20'] = df_f0_t1.shape[0]
                cel['1_J_20'] = 0
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
                cel['2_F_20'] = 0
                cel['2_G_20'] = df_f_to['26'].sum()
                cel['2_H_20'] = cel['2_G_20']

                # TIPO 1
                df_f_t1 = df_f[df_f['19'] == 1]
                cel['2_I_20'] = df_f_t1.shape[0]
                cel['2_J_20'] = 0
                cel['2_K_20'] = df_f_t1['26'].sum()
                cel['2_L_20'] = cel['2_K_20']

                # TOTAL
                cel['2_M_20'] = cel['2_E_20'] + cel['2_I_20']
                cel['2_N_20'] = cel['2_F_20'] + cel['2_J_20']
                cel['2_O_20'] = cel['2_G_20'] + cel['2_K_20']
                cel['2_P_20'] = cel['2_O_20']

                # # FINANCIADO 100% # #
                df_f100 = df[(df['27'] == 100) & ((df['19'] == 0) | (df['19'] == 1))]

                cel['3_E_20'] = df_f100.shape[0]
                cel['3_F_20'] = 0
                cel['3_G_20'] = df_f100['26'].sum()
                cel['3_H_20'] = cel['3_G_20']

                for k, v in cel.items():
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ###########
                # # CTs # #
                ###########

                file_path = '/tmp/8_2021_loaded_or_generated_b2.txt'
                columns = [str(x) for x in range(36)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns)
                df['31'] = pd.to_numeric(df['31'], errors='coerce').astype(float)
                df['23'] = pd.to_numeric(df['23'], errors='coerce').astype(float)
                df['8'] = pd.to_numeric(df['8'], errors='coerce').astype(float)
                ct = OrderedDict()

                # CUADRO 5
                resumen['5_G_6'] += df['27'].sum()
                resumen['5_G_7'] += df['29'].sum()
                resumen['5_G_8'] += df['28'].sum()

                ct_names = names['giscedata.cts']
                df_5_g_10 = df[df['0'].isin(ct_names)]
                resumen['5_G_9'] += df_5_g_10['30'].sum()

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
                df_f100 = df[(df['31'] == 100) & ((df['23'] == 0) | (df['23'] == 1))]

                ct['3_E_21'] = df_f100.shape[0]
                ct['3_F_21'] = df_f100['8'].sum() / 1000
                ct['3_G_21'] = df_f100['30'].sum()
                ct['3_H_21'] = ct['3_G_21']

                for k, v in ct.items():
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
                inv_uf['1_E_22'] = at['1_E_16'] + bt['1_E_17'] + pos['1_E_18'] + trafo['1_E_19'] + cel['1_E_20'] + ct['1_E_21']
                inv_uf['1_F_22'] = at['1_F_16'] + bt['1_F_17'] + trafo['1_F_19'] + ct['1_F_21']
                inv_uf['1_G_22'] = at['1_G_16'] + bt['1_G_17'] + pos['1_G_18'] + trafo['1_G_19'] + cel['1_G_20'] + ct['1_G_21']
                inv_uf['1_H_22'] = at['1_H_16'] + bt['1_H_17'] + pos['1_H_18'] + trafo['1_H_19'] + cel['1_H_20'] + ct['1_H_21']

                # TIPO 1
                inv_uf['1_I_22'] = at['1_I_16'] + bt['1_I_17'] + pos['1_I_18'] + trafo['1_I_19'] + cel['1_I_20'] + ct['1_I_21']
                inv_uf['1_J_22'] = at['1_J_16'] + bt['1_J_17'] + trafo['1_J_19'] + ct['1_J_21']
                inv_uf['1_K_22'] = at['1_K_16'] + bt['1_K_17'] + pos['1_K_18'] + trafo['1_K_19'] + cel['1_K_20'] + ct['1_K_21']
                inv_uf['1_L_22'] = at['1_L_16'] + bt['1_L_17'] + pos['1_L_18'] + trafo['1_L_19'] + cel['1_L_20'] + ct['1_L_21']

                # TOTAL
                inv_uf['1_M_22'] = at['1_M_16'] + bt['1_M_17'] + pos['1_M_18'] + trafo['1_M_19'] + cel['1_M_20'] + ct['1_M_21']
                inv_uf['1_N_22'] = at['1_N_16'] + bt['1_N_17'] + trafo['1_N_19'] + ct['1_N_21']
                inv_uf['1_O_22'] = at['1_O_16'] + bt['1_O_17'] + pos['1_O_18'] + trafo['1_O_19'] + cel['1_O_20'] + ct['1_O_21']
                inv_uf['1_P_22'] = at['1_P_16'] + bt['1_P_17'] + pos['1_P_18'] + trafo['1_P_19'] + cel['1_P_20'] + ct['1_P_21']

                # # 0% < FINANCIADO < 100% # #

                # TIPO 0
                inv_uf['2_E_22'] = at['2_E_16'] + bt['2_E_17'] + pos['2_E_18'] + trafo['2_E_19'] + cel['2_E_20'] + ct['2_E_21']
                inv_uf['2_F_22'] = at['2_F_16'] + bt['2_F_17'] + trafo['2_F_19'] + ct['2_F_21']
                inv_uf['2_G_22'] = at['2_G_16'] + bt['2_G_17'] + pos['2_G_18'] + trafo['2_G_19'] + cel['2_G_20'] + ct['2_G_21']
                inv_uf['2_H_22'] = at['2_H_16'] + bt['2_H_17'] + pos['2_H_18'] + trafo['2_H_19'] + cel['2_H_20'] + ct['2_H_21']

                # TIPO 1
                inv_uf['2_I_22'] = at['2_I_16'] + bt['2_I_17'] + pos['2_I_18'] + trafo['2_I_19'] + cel['2_I_20'] + ct['2_I_21']
                inv_uf['2_J_22'] = at['2_J_16'] + bt['2_J_17'] + trafo['2_J_19'] + ct['2_J_21']
                inv_uf['2_K_22'] = at['2_K_16'] + bt['2_K_17'] + pos['2_K_18'] + trafo['2_K_19'] + cel['2_K_20'] + ct['2_K_21']
                inv_uf['2_L_22'] = at['2_L_16'] + bt['2_L_17'] + pos['2_L_18'] + trafo['2_L_19'] + cel['2_L_20'] + ct['2_L_21']

                # TOTAL
                inv_uf['2_M_22'] = at['2_M_16'] + bt['2_M_17'] + pos['2_M_18'] + trafo['2_M_19'] + cel['2_M_20'] + ct['2_M_21']
                inv_uf['2_N_22'] = at['2_N_16'] + bt['2_N_17'] + trafo['2_N_19'] + ct['2_N_21']
                inv_uf['2_O_22'] = at['2_O_16'] + bt['2_O_17'] + pos['2_O_18'] + trafo['2_O_19'] + cel['2_O_20'] + ct['2_O_21']
                inv_uf['2_P_22'] = at['2_P_16'] + bt['2_P_17'] + pos['2_P_18'] + trafo['2_P_19'] + cel['2_P_20'] + ct['2_P_21']

                # # FINANCIADO 100% # #
                inv_uf['3_E_22'] = at['3_E_16'] + bt['3_E_17'] + pos['3_E_18'] + trafo['3_E_19'] + cel['3_E_20'] + ct['3_E_21']
                inv_uf['3_F_22'] = at['3_F_16'] + bt['3_F_17'] + trafo['3_F_19'] + ct['3_F_21']
                inv_uf['3_G_22'] = at['3_G_16'] + bt['3_G_17'] + pos['3_G_18'] + trafo['3_G_19'] + cel['3_G_20'] + ct['3_G_21']
                inv_uf['3_H_22'] = at['3_H_16'] + bt['3_H_17'] + pos['3_H_18'] + trafo['3_H_19'] + cel['3_H_20'] + ct['3_H_21']

                for k, v in inv_uf.items():
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])


                #########################
                # # EQUIPOS DE MEDIDA # #
                #########################

                equipos = OrderedDict()

                equipos_obj = O.GiscedataInversionsEquipsMesura
                equipos_id = equipos_obj.search([('year', '=', self.year)])
                fields_to_read = [
                    '1a_d_10', '1a_e_10', 'tipo_10',
                    '1a_d_11', '1a_e_11', 'tipo_11',
                    '1a_d_12', '1a_e_12', 'tipo_12',
                    '1a_d_13', '1a_e_13', 'tipo_13',
                    '1a_d_14', '1a_e_14', 'tipo_14',
                    '1a_d_15', '1a_e_15', 'tipo_15',
                    '1a_d_16', '1a_e_16', 'tipo_16',
                    '1a_d_17', '1a_e_17', 'tipo_17',
                    '1a_d_18', '1a_e_18', 'tipo_18',
                    '1a_d_19', '1a_e_19', 'tipo_19'
                ]
                if equipos_id:
                    equipos_data = equipos_obj.read(equipos_id[0], fields_to_read)

                    # # # CUADRO 1A # # #

                    # # EN SERVICIO CLIENTE ##

                    # TIPO 1 #
                    equipos['1A_D_10'] = equipos_data['1a_d_10']
                    equipos['1A_E_10'] = equipos_data['1a_e_10']
                    # TIPO 2 #
                    equipos['1A_D_11'] = equipos_data['1a_d_11']
                    equipos['1A_E_11'] = equipos_data['1a_e_11']
                    # TIPO 3 BT #
                    equipos['1A_D_12'] = equipos_data['1a_d_12']
                    equipos['1A_E_12'] = equipos_data['1a_e_12']
                    # TIPO 3 AT #
                    equipos['1A_D_13'] = equipos_data['1a_d_13']
                    equipos['1A_E_13'] = equipos_data['1a_e_13']
                    # TIPO 4 con telegestión #
                    equipos['1A_D_14'] = equipos_data['1a_d_14']
                    equipos['1A_E_14'] = equipos_data['1a_e_14']
                    # TIPO 4 sin telegestión #
                    equipos['1A_D_15'] = equipos_data['1a_d_15']
                    equipos['1A_E_15'] = equipos_data['1a_e_15']
                    # TIPO 5 #
                    equipos['1A_D_16'] = equipos_data['1a_d_16']
                    equipos['1A_E_16'] = equipos_data['1a_e_16']
                    # TODOS #
                    equipos['1A_D_17'] = equipos['1A_D_10'] + equipos['1A_D_11'] + equipos['1A_D_12'] + equipos['1A_D_13'] \
                                         + equipos['1A_D_14'] + equipos['1A_D_15'] + equipos['1A_D_16']
                    equipos['1A_E_17'] = equipos['1A_E_10'] + equipos['1A_E_11'] + equipos['1A_E_12'] + equipos['1A_E_13'] \
                                         + equipos['1A_E_14'] + equipos['1A_E_15'] + equipos['1A_E_16']

                    # # EN RED # #

                    equipos['1A_D_18'] = equipos_data['1a_d_18']
                    equipos['1A_E_18'] = equipos_data['1a_e_18']

                    # # TOTAL # #

                    # # # CUADRO 1 - G # # #
                    equipos['1A_D_19'] = equipos['1A_D_17'] + equipos['1A_D_18']
                    equipos['1A_E_19'] = equipos['1A_E_17'] + equipos['1A_E_18']

                    equipos['1_E_23'] = 0
                    equipos['1_G_23'] = 0
                    equipos['1_I_23'] = 0
                    equipos['1_K_23'] = 0
                    if equipos_data['tipo_10'] == 'tipo_0':
                        equipos['1_E_23'] += equipos['1A_D_10']
                        equipos['1_G_23'] += equipos['1A_E_10']
                    elif equipos_data['tipo_10'] == 'tipo_1':
                        equipos['1_I_23'] += equipos['1A_D_10']
                        equipos['1_K_23'] += equipos['1A_E_10']
                    if equipos_data['tipo_11'] == 'tipo_0':
                        equipos['1_E_23'] += equipos['1A_D_11']
                        equipos['1_G_23'] += equipos['1A_E_11']
                    elif equipos_data['tipo_11'] == 'tipo_1':
                        equipos['1_I_23'] += equipos['1A_D_11']
                        equipos['1_K_23'] += equipos['1A_E_11']
                    if equipos_data['tipo_12'] == 'tipo_0':
                        equipos['1_E_23'] += equipos['1A_D_12']
                        equipos['1_G_23'] += equipos['1A_E_12']
                    elif equipos_data['tipo_12'] == 'tipo_1':
                        equipos['1_I_23'] += equipos['1A_D_12']
                        equipos['1_K_23'] += equipos['1A_E_12']
                    if equipos_data['tipo_13'] == 'tipo_0':
                        equipos['1_E_23'] += equipos['1A_D_13']
                        equipos['1_G_23'] += equipos['1A_E_13']
                    elif equipos_data['tipo_13'] == 'tipo_1':
                        equipos['1_I_23'] += equipos['1A_D_13']
                        equipos['1_K_23'] += equipos['1A_E_13']
                    if equipos_data['tipo_14'] == 'tipo_0':
                        equipos['1_E_23'] += equipos['1A_D_14']
                        equipos['1_G_23'] += equipos['1A_E_14']
                    elif equipos_data['tipo_14'] == 'tipo_1':
                        equipos['1_I_23'] += equipos['1A_D_14']
                        equipos['1_K_23'] += equipos['1A_E_14']
                    if equipos_data['tipo_15'] == 'tipo_0':
                        equipos['1_E_23'] += equipos['1A_D_15']
                        equipos['1_G_23'] += equipos['1A_E_15']
                    elif equipos_data['tipo_15'] == 'tipo_1':
                        equipos['1_I_23'] += equipos['1A_D_15']
                        equipos['1_K_23'] += equipos['1A_E_15']
                    if equipos_data['tipo_16'] == 'tipo_0':
                        equipos['1_E_23'] += equipos['1A_D_16']
                        equipos['1_G_23'] += equipos['1A_E_16']
                    elif equipos_data['tipo_16'] == 'tipo_1':
                        equipos['1_I_23'] += equipos['1A_D_16']
                        equipos['1_K_23'] += equipos['1A_E_16']
                    if equipos_data['tipo_18'] == 'tipo_0':
                        equipos['1_E_23'] += equipos['1A_D_18']
                        equipos['1_G_23'] += equipos['1A_E_18']
                    elif equipos_data['tipo_18'] == 'tipo_1':
                        equipos['1_I_23'] += equipos['1A_D_18']
                        equipos['1_K_23'] += equipos['1A_E_18']

                    # TIPO 0 #
                    equipos['1_F_23'] = 0
                    equipos['1_H_23'] = equipos['1_G_23']

                    # TIPO 1 #
                    equipos['1_J_23'] = 0
                    equipos['1_L_23'] = equipos['1_K_23']

                    # TOTAL #
                    equipos['1_M_23'] = equipos['1_E_23'] + equipos['1_I_23']
                    equipos['1_N_23'] = equipos['1_F_23'] + equipos['1_J_23']
                    equipos['1_O_23'] = equipos['1_G_23'] + equipos['1_K_23']
                    equipos['1_P_23'] = equipos['1_O_23']

                else:
                    equipos['1A_D_10'] = 0
                    equipos['1A_E_10'] = 0
                    equipos['1A_D_11'] = 0
                    equipos['1A_E_11'] = 0
                    equipos['1A_D_12'] = 0
                    equipos['1A_E_12'] = 0
                    equipos['1A_D_13'] = 0
                    equipos['1A_E_13'] = 0
                    equipos['1A_D_14'] = 0
                    equipos['1A_E_14'] = 0
                    equipos['1A_D_15'] = 0
                    equipos['1A_E_15'] = 0
                    equipos['1A_D_16'] = 0
                    equipos['1A_E_16'] = 0
                    equipos['1A_D_17'] = 0
                    equipos['1A_E_17'] = 0
                    equipos['1A_D_18'] = 0
                    equipos['1A_E_18'] = 0
                    equipos['1A_D_19'] = 0
                    equipos['1A_E_19'] = 0
                    equipos['1_E_23'] = 0
                    equipos['1_G_23'] = 0
                    equipos['1_I_23'] = 0
                    equipos['1_K_23'] = 0
                    equipos['1_E_23'] = 0
                    equipos['1_G_23'] = 0
                    equipos['1_I_23'] = 0
                    equipos['1_K_23'] = 0
                    equipos['1_E_23'] = 0
                    equipos['1_G_23'] = 0
                    equipos['1_I_23'] = 0
                    equipos['1_K_23'] = 0
                    equipos['1_E_23'] = 0
                    equipos['1_G_23'] = 0
                    equipos['1_I_23'] = 0
                    equipos['1_K_23'] = 0
                    equipos['1_E_23'] = 0
                    equipos['1_G_23'] = 0
                    equipos['1_I_23'] = 0
                    equipos['1_K_23'] = 0
                    equipos['1_E_23'] = 0
                    equipos['1_G_23'] = 0
                    equipos['1_I_23'] = 0
                    equipos['1_K_23'] = 0
                    equipos['1_E_23'] = 0
                    equipos['1_G_23'] = 0
                    equipos['1_I_23'] = 0
                    equipos['1_K_23'] = 0
                    equipos['1_E_23'] = 0
                    equipos['1_G_23'] = 0
                    equipos['1_I_23'] = 0
                    equipos['1_K_23'] = 0
                    equipos['1_E_23'] = 0
                    equipos['1_G_23'] = 0
                    equipos['1_I_23'] = 0
                    equipos['1_K_23'] = 0

                    # TIPO 0 #
                    equipos['1_F_23'] = 0
                    equipos['1_H_23'] = equipos['1_G_23']

                    # TIPO 1 #
                    equipos['1_J_23'] = 0
                    equipos['1_L_23'] = equipos['1_K_23']

                    # TOTAL #
                    equipos['1_M_23'] = equipos['1_E_23'] + equipos['1_I_23']
                    equipos['1_N_23'] = equipos['1_F_23'] + equipos['1_J_23']
                    equipos['1_O_23'] = equipos['1_G_23'] + equipos['1_K_23']
                    equipos['1_P_23'] = equipos['1_O_23']

                # # # CUADRO 2 - G # # #
                equipos['2_E_23'] = 0
                equipos['2_F_23'] = 0
                equipos['2_G_23'] = 0
                equipos['2_H_23'] = 0
                equipos['2_I_23'] = 0
                equipos['2_J_23'] = 0
                equipos['2_K_23'] = 0
                equipos['2_L_23'] = 0
                equipos['2_M_23'] = 0
                equipos['2_N_23'] = 0
                equipos['2_O_23'] = 0
                equipos['2_P_23'] = 0

                # # # CUADRO 3 - G # # #
                equipos['3_E_23'] = 0
                equipos['3_F_23'] = 0
                equipos['3_G_23'] = 0
                equipos['3_H_23'] = 0

                for k, v in equipos.items():
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ###########################
                # # OTROS INMOVILIZADOS # #
                ###########################

                file_path = '/tmp/8_2021_loaded_or_generated_b8.txt'
                columns = [str(x) for x in range(17)]
                df = pd.read_csv(file_path, sep=';', decimal=',', names=columns, dtype={'2': 'object'})
                df['15'] = pd.to_numeric(df['15'], errors='coerce').astype(float)
                desp = OrderedDict()

                resumen['5_G_6'] += df['11'].sum()
                resumen['5_G_7'] += df['13'].sum()
                resumen['5_G_8'] += df['12'].sum()

                # # # CUADRO 1B # # #

                # # FINANCIADO 0% # #
                df_f0 = df[df['15'] == 0]

                # NO DIGITALIZACION #
                df_f0_des = df_f0[df_f0['2'] == 'DES']
                desp['1B_D_13'] = df_f0_des.shape[0]
                desp['1B_E_13'] = df_f0_des['14'].sum()
                desp['1B_F_13'] = desp['1B_E_13']

                # TERRENOS #
                df_f0_ter = df_f0[df_f0['2'] == 'TER']
                desp['1B_D_14'] = df_f0_ter.shape[0]
                desp['1B_E_14'] = df_f0_ter['14'].sum()
                desp['1B_F_14'] = desp['1B_E_14']

                # DIGITALIZACION #
                df_f0_dig = df_f0[df_f0['2'] == 'DIG']
                desp['1B_D_15'] = df_f0_dig.shape[0]
                desp['1B_E_15'] = df_f0_dig['14'].sum()
                desp['1B_F_15'] = desp['1B_E_15']

                # PRTR #
                desp['1B_D_16'] = 0
                desp['1B_E_16'] = 0
                desp['1B_F_16'] = 0

                # OTRO IBO #
                df_f0_ibo = df_f0[df_f0['2'] == 'IBO']
                desp['1B_D_17'] = df_f0_ibo.shape[0]
                desp['1B_E_17'] = df_f0_ibo['14'].sum()
                desp['1B_F_17'] = desp['1B_E_17']

                # TOTAL #
                desp['1B_D_18'] = desp['1B_D_13'] + desp['1B_D_14'] + desp['1B_D_15'] + desp['1B_D_16'] + desp['1B_D_17']
                desp['1B_E_18'] = desp['1B_E_13'] + desp['1B_E_14'] + desp['1B_E_15'] + desp['1B_E_16'] + desp['1B_E_17']
                desp['1B_F_18'] = desp['1B_F_13'] + desp['1B_F_14'] + desp['1B_F_15'] + desp['1B_F_16'] + desp['1B_F_17']

                # # # CUADRO 1 - H # # #

                desp['1_E_24'] = desp['1B_D_18']
                desp['1_F_24'] = 0
                desp['1_G_24'] = desp['1B_E_18']
                desp['1_H_24'] = desp['1B_F_18']

                desp['1_I_24'] = 0
                desp['1_J_24'] = 0
                desp['1_K_24'] = 0
                desp['1_L_24'] = 0

                desp['1_M_24'] = desp['1_E_24']
                desp['1_N_24'] = desp['1_F_24']
                desp['1_O_24'] = desp['1_G_24']
                desp['1_P_24'] = desp['1_H_24']

                # # # CUADRO 2A # # #

                # # 0% < FINANCIADO < 100% # #
                df_f = df[(0 < df['15']) & (df['15'] < 100)]

                # NO DIGITALIZACION #
                df_f_des = df_f[df_f['2'] == 'DES']
                desp['2A_D_13'] = df_f_des.shape[0]
                desp['2A_E_13'] = df_f_des['14'].sum()
                desp['2A_F_13'] = desp['2A_E_13']

                # TERRENOS #
                df_f_ter = df_f[df_f['2'] == 'TER']
                desp['2A_D_14'] = df_f_ter.shape[0]
                desp['2A_E_14'] = df_f_ter['14'].sum()
                desp['2A_F_14'] = desp['2A_E_14']

                # DIGITALIZACION #
                df_f_dig = df_f[df_f['2'] == 'DIG']
                desp['2A_D_15'] = df_f_dig.shape[0]
                desp['2A_E_15'] = df_f_dig['14'].sum()
                desp['2A_F_15'] = desp['2A_E_15']

                # PRTR #
                desp['2A_D_16'] = 0
                desp['2A_E_16'] = 0
                desp['2A_F_16'] = 0

                # OTRO IBO #
                df_f_ibo = df_f[df_f['2'] == 'IBO']
                desp['2A_D_17'] = df_f_ibo.shape[0]
                desp['2A_E_17'] = df_f_ibo['14'].sum()
                desp['2A_F_17'] = desp['2A_E_17']

                # TOTAL #
                desp['2A_D_18'] = desp['2A_D_13'] + desp['2A_D_14'] + desp['2A_D_15'] + desp['2A_D_16'] + desp['2A_D_17']
                desp['2A_E_18'] = desp['2A_E_13'] + desp['2A_E_14'] + desp['2A_E_15'] + desp['2A_E_16'] + desp['2A_E_17']
                desp['2A_F_18'] = desp['2A_F_13'] + desp['2A_F_14'] + desp['2A_F_15'] + desp['2A_F_16'] + desp['2A_F_17']

                desp['2_E_24'] = desp['2A_D_18']
                desp['2_F_24'] = 0
                desp['2_G_24'] = desp['2A_E_18']
                desp['2_H_24'] = desp['2A_F_18']

                # # # CUADRO 2 - H # # #

                desp['2_I_24'] = 0
                desp['2_J_24'] = 0
                desp['2_K_24'] = 0
                desp['2_L_24'] = 0

                desp['2_M_24'] = desp['2_E_24']
                desp['2_N_24'] = desp['2_F_24']
                desp['2_O_24'] = desp['2_G_24']
                desp['2_P_24'] = desp['2_H_24']

                # # # CUADRO 3 - H # # #

                # # FINANCIADO 100% # #
                df_f100 = df[(df['15'] == 100) & (df['14'] >= 0)]

                desp['3_E_24'] = df_f100.shape[0]
                desp['3_F_24'] = 0
                desp['3_G_24'] = df_f100['14'].sum()
                desp['3_H_24'] = desp['3_G_24']

                for k, v in desp.items():
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
                inv['1_E_25'] = inv_uf['1_E_22'] + equipos['1_E_23'] + desp['1_E_24']
                inv['1_F_25'] = inv_uf['1_F_22']
                inv['1_G_25'] = inv_uf['1_G_22'] + equipos['1_G_23'] + desp['1_G_24']
                inv['1_H_25'] = inv_uf['1_H_22'] + equipos['1_H_23'] + desp['1_H_24']

                # TIPO 1
                inv['1_I_25'] = inv_uf['1_I_22'] + equipos['1_I_23']
                inv['1_J_25'] = inv_uf['1_J_22']
                inv['1_K_25'] = inv_uf['1_K_22'] + equipos['1_K_23']
                inv['1_L_25'] = inv_uf['1_L_22'] + equipos['1_L_23']

                # TOTAL
                inv['1_M_25'] = inv_uf['1_M_22'] + equipos['1_M_23'] + desp['1_M_24']
                inv['1_N_25'] = inv_uf['1_N_22']
                inv['1_O_25'] = inv_uf['1_O_22'] + equipos['1_O_23'] + desp['1_O_24']
                inv['1_P_25'] = inv_uf['1_P_22'] + equipos['1_P_23'] + desp['1_P_24']

                # # 0% < FINANCIADO < 100% # #

                # TIPO 0
                inv['2_E_25'] = inv_uf['2_E_22'] + desp['2_E_24']
                inv['2_F_25'] = inv_uf['2_F_22']
                inv['2_G_25'] = inv_uf['2_G_22'] + desp['2_G_24']
                inv['2_H_25'] = inv_uf['2_H_22'] + desp['2_H_24']

                # TIPO 1
                inv['2_I_25'] = inv_uf['2_I_22']
                inv['2_J_25'] = inv_uf['2_J_22']
                inv['2_K_25'] = inv_uf['2_K_22']
                inv['2_L_25'] = inv_uf['2_L_22']

                # TOTAL
                inv['2_M_25'] = inv_uf['2_M_22'] + desp['2_M_24']
                inv['2_N_25'] = inv_uf['2_N_22']
                inv['2_O_25'] = inv_uf['2_O_22'] + desp['2_O_24']
                inv['2_P_25'] = inv_uf['2_P_22'] + desp['2_P_24']

                # # FINANCIADO 100% # #
                inv['3_E_25'] = inv_uf['3_E_22'] + desp['3_E_24']
                inv['3_F_25'] = inv_uf['3_F_22']
                inv['3_G_25'] = inv_uf['3_G_22'] + desp['3_G_24']
                inv['3_H_25'] = inv_uf['3_H_22'] + desp['3_H_24']

                for k, v in inv.items():
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ################
                # # CUADRO 4 # #
                ################

                autonomica = OrderedDict()

                autonomica['4_E_17'] = 0
                autonomica['4_F_17'] = 0
                autonomica['4_G_17'] = 0
                autonomica['4_H_17'] = 0
                autonomica['4_E_18'] = 0
                autonomica['4_F_18'] = 0
                autonomica['4_G_18'] = 0
                autonomica['4_H_18'] = 0
                autonomica['4_E_19'] = 0
                autonomica['4_F_19'] = 0
                autonomica['4_G_19'] = 0
                autonomica['4_H_19'] = 0
                autonomica['4_E_20'] = 0
                autonomica['4_F_20'] = 0
                autonomica['4_G_20'] = 0
                autonomica['4_H_20'] = 0
                autonomica['4_E_21'] = 0
                autonomica['4_F_21'] = 0
                autonomica['4_G_21'] = 0
                autonomica['4_H_21'] = 0
                autonomica['4_E_22'] = 0
                autonomica['4_F_22'] = 0
                autonomica['4_G_22'] = 0
                autonomica['4_H_22'] = 0
                autonomica['4_E_23'] = 0
                autonomica['4_F_23'] = 0
                autonomica['4_G_23'] = 0
                autonomica['4_H_23'] = 0
                autonomica['4_E_24'] = 0
                autonomica['4_F_24'] = 0
                autonomica['4_G_24'] = 0
                autonomica['4_H_24'] = 0
                autonomica['4_E_25'] = 0
                autonomica['4_F_25'] = 0
                autonomica['4_G_25'] = 0
                autonomica['4_H_25'] = 0
                autonomica['4_E_26'] = 0
                autonomica['4_F_26'] = 0
                autonomica['4_G_26'] = 0
                autonomica['4_H_26'] = 0

                for k, v in autonomica.items():
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                ###############
                # # RESUMEN # #
                ###############

                # INVERSIÓN BRUTA
                resumen['5_G_5'] = inv['1_O_25'] + inv['2_O_25'] + inv['3_G_25']

                # INVERSIÓN INSTALACIONES FINANCIADAS
                resumen['5_G_8'] = inv['3_G_25']

                # INVERSIÓN EQUIPOS MEDIDA
                resumen['5_G_10'] = equipos['1A_E_17']

                # INVERSIÓN NETA TOTAL
                resumen['5_G_11'] = resumen['5_G_5'] - resumen['5_G_6'] - resumen['5_G_7'] - resumen['5_G_8'] \
                                    - resumen['5_G_9'] - resumen['5_G_10']

                # INGRESOS PERCIBIDOS
                resumen['5_G_12'] = 0
                c3_obj = self.connection.model('cir8.2021.c3')
                c3_ids = c3_obj.search([('year', '=', self.year)])

                if c3_ids:
                    for c3_id in c3_ids:
                        ingreso_data = c3_obj.read(c3_id, ['ingreso'])
                        if ingreso_data.get('ingreso', False):
                            resumen['5_G_12'] += ingreso_data['ingreso']

                for k, v in resumen.items():
                    self.output_q.put([
                        k,                                 # CODIGO_CELDA
                        self.format_f(v, 2),               # IMPORTE
                    ])

                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
