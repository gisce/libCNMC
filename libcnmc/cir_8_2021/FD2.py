#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Informe de la CNMC relatiu a la generació conectada de les xarxes de distribució
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
from workalendar.europe import Spain



class FD2(MultiprocessBased):

    def __init__(self, **kwargs):
        super(FD2, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1') or ''
        self.year = kwargs.pop('year', datetime.now().year - 2)
        self.report_name = 'FD2 - Calidad Comercial'
        self.base_object = 'Despatxos'

    def get_sequence(self):

        """
            Retorna la llista de ID's per al metode consumer.
            :return: List of ID's
            :rtype: list(int)
        """

        search_params_interns = [
            ('name', 'like', '(intern)')
        ]
        intern_z = self.connection.GiscedataCodigosGestionCalidadZ.search(search_params_interns)
        search_params = [
            ('id', 'not in', intern_z)
        ]
        return self.connection.GiscedataCodigosGestionCalidadZ.search(search_params)

    def compute_time_atc(self, crm_id, cod_gest_data, values, context=None):
        if context is None:
            context = {}
        o = self.connection
        state = o.CrmCase.read(crm_id, ['state'])['state']
        if state == 'done':
            history_logs = o.CrmCase.read(crm_id, ['history_line'])['history_line']
            total_ts = 0
            for history_log in history_logs:
                tt_id = o.CrmCaseHistory.read(history_log, ['time_tracking_id'])['time_tracking_id']
                if tt_id and tt_id[1] == 'Distribuidora':
                    time_spent = o.CrmCaseHistory.read(history_log, ['time_spent'])['time_spent']
                    total_ts = total_ts + time_spent
            if total_ts != 0:
                if total_ts > cod_gest_data['dies_limit']:
                    values['fuera_plazo'] = values['fuera_plazo'] + 1
                else:
                    values['dentro_plazo'] = values['dentro_plazo'] + 1
        else:
            values['no_tramitadas'] = values['no_tramitadas'] + 1
        values['totals'] = values['totals'] + 1

    def consumer(self):

        o = self.connection

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                file_fields = {'totals': 0, 'dentro_plazo': 0, 'fuera_plazo': 0, 'no_tramitadas': 0 }
                z8_fields = {'totals': 0, 'dentro_plazo': 0, 'fuera_plazo': 0, 'no_tramitadas': 0 }

                year_start = '01-01-' + str(self.year)
                year_end = '12-31-' + str(self.year)
                cod_gest_data = o.GiscedataCodigosGestionCalidadZ.read(item, ['dies_limit', 'name'])
                z_type, z_subtype = cod_gest_data['name'].split('_')
                if 'Z4' in z_type:
                    search_params = [
                        ('create_date', '>=', year_start),
                        ('create_date', '<=', year_end)
                    ]
                    r1_ids = o.model("giscedata.switching.r1.02").search(search_params)
                    if '03' in z_subtype:

                        for r1_id in r1_ids:
                            r1_header_id = o.GiscedataSwitchingR102.read(r1_id, ['header_id'])['header_id']
                            sw_id = o.GiscedataSwitchingStepHeader.read(r1_header_id, ['sw_id'])['sw_id']
                            step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])
                            proces_name = o.GiscedataSwtichingStep(step_id, ['name'])
                            if proces_name is '05':
                                r105_id = o.GiscedataSwitchingR105.search([('sw_id', '=', sw_id)])
                                if r105_id:
                                    raw_date_05 = o.model("giscedata.switching.r1.05").read(r105_id, ['create_date'])['create_date']
                                    raw_date_02 = o.model("giscedata.switching.r1.02").read(r1_id, ['create_date'])['create_date']
                                    date_05 = raw_date_05.strptime(raw_date_05.split(' ')[0], "%Y-%m-%d")
                                    date_02 = raw_date_02.strptime(raw_date_02.split(' ')[0], "%Y-%m-%d")
                                    time_spent = Spain().get_working_days_delta(date_02, date_05)
                                    if time_spent > cod_gest_data['dies_limit']:
                                        file_fields['fuera_plazo'] = file_fields['fuera_plazo'] + 1
                                    else:
                                        file_fields['dentro_plazo'] = file_fields['dentro_plazo'] + 1
                            else:
                                file_fields['no_tramitadas'] = file_fields['no_tramitadas'] + 1
                            file_fields['totals'] = file_fields['totals'] + 1
                    else:
                        subtipus_ids = o.GiscedataSubtipusReclamacio.search([('name', 'in', ['008', '009', '028'])])
                        search_params = [
                            ('create_date', '>=', year_start),
                            ('create_date', '<=', year_end),
                            ('state', '!=', 'cancel'),
                            ('subtipus_id', 'in', subtipus_ids)
                        ]
                        atc_ids = o.GiscedataAtc.search(search_params)
                        for atc_id in atc_ids:
                            crm_id = o.GiscedataAtc.read(atc_id, ['crm_id'])['crm_id'][0]
                            self.compute_time_atc(crm_id, cod_gest_data, file_fields, context={})
                else:
                    search_params_atc = [
                        ('create_date', '>=', year_start),
                        ('create_date', '<=', year_end),
                        ('cod_gestion_id', '=', item),
                        ('state', '!=', 'cancel')
                    ]
                    atc_ids = o.GiscedataAtc.search(search_params_atc)
                    cod_gest_data = o.GiscedataCodigosGestionCalidadZ.read(item, ['dies_limit', 'name'])
                    for atc_id in atc_ids:
                        crm_id = o.GiscedataAtc.read(atc_id, ['crm_id'])['crm_id'][0]
                        if 'Z8_01' in cod_gest_data['name']:
                            self.compute_time_atc(crm_id, cod_gest_data, z8_fields, context={})
                        else:
                            self.compute_time_atc(crm_id, cod_gest_data, file_fields, context={})

                if 'Z8_01' not in cod_gest_data['name']:
                    output = [
                        cod_gest_data['name'],
                        file_fields['totals'],
                        file_fields['dentro_plazo'],
                        file_fields['fuera_plazo'],
                        file_fields['no_tramitadas']
                    ]
                    self.output_q.put(output)
                elif cod_gest_data['name'] == 'Z8_01_dl15':
                    output = [
                        'Z8_01',
                        z8_fields['totals'],
                        z8_fields['dentro_plazo'],
                        z8_fields['fuera_plazo'],
                        z8_fields['no_tramitadas']
                    ]
                    self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
