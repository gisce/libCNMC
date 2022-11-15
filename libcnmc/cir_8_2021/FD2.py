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


def compute_time(cod_gest_data, values, time_delta):

    if time_delta > cod_gest_data['dies_limit']:
        values['fuera_plazo'] = values['fuera_plazo'] + 1
    else:
        values['dentro_plazo'] = values['dentro_plazo'] + 1
    values['totals'] = values['totals'] + 1


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

    def get_atc_time_delta(self, crm_id, total_ts, context=None):
        if context is None:
            context = {}
        o = self.connection

        history_logs = o.CrmCase.read(crm_id, ['history_line'])['history_line']
        for history_log in history_logs:
            tt_id = o.CrmCaseHistory.read(history_log, ['time_tracking_id'])['time_tracking_id']
            if tt_id and tt_id[1] == 'Distribuidora':
                time_spent = o.CrmCaseHistory.read(history_log, ['time_spent'])['time_spent']
                total_ts = total_ts + time_spent

        return total_ts

    def get_r1_time_delta(self, r102_id, r105_id, model_name):

        o = self.connection

        raw_date_05 = o.model(model_name[0]).read(r105_id, ['date_created'])['date_created']
        raw_date_02 = o.model(model_name[1]).read(r102_id, ['date_created'])['date_created']
        date_05 = datetime.strptime(raw_date_05.split(' ')[0], "%Y-%m-%d")
        date_02 = datetime.strptime(raw_date_02.split(' ')[0], "%Y-%m-%d")

        return Spain().get_working_days_delta(date_02, date_05)

    def consumer(self):

        o = self.connection

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                file_fields = {'totals': 0, 'dentro_plazo': 0, 'fuera_plazo': 0, 'no_tramitadas': 0}
                z8_fields = {'totals': 0, 'dentro_plazo': 0, 'fuera_plazo': 0, 'no_tramitadas': 0}

                year_start = '01-01-' + str(self.year)
                year_end = '12-31-' + str(self.year)
                cod_gest_data = o.GiscedataCodigosGestionCalidadZ.read(item, ['dies_limit', 'name'])

                ## Tractem el codi de gestio Z4
                if 'Z4' in cod_gest_data['name']:
                    if '03' in cod_gest_data['name']:
                        search_params = [
                            ('date_created', '>=', year_start),
                            ('date_created', '<=', year_end)
                        ]
                        r102_ids = o.model("giscedata.switching.r1.02").search(search_params)
                        ## Tractem els r1 i comptabilitzem els que escau
                        for r102_id in r102_ids:
                            r1_header_id = o.model("giscedata.switching.r1.02").read(r102_id, ['header_id'])[
                                'header_id']
                            sw_id = o.GiscedataSwitchingStepHeader.read(r1_header_id[0], ['sw_id'])['sw_id'][0]
                            step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                            proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                            if '05' in proces_name:
                                r105_id = o.model('giscedata.switching.r1.05').search([('sw_id', '=', sw_id)])[0]
                                if r105_id:
                                    enviament_pendent = o.model('giscedata.switching.r1.05').read(r105_id,
                                        ['enviament_pendent'])['enviament_pendent']
                                    if not enviament_pendent:
                                        model_names = ['giscedata.switching.r1.05', 'giscedata.switching.r1.02']
                                        time_spent = self.get_r1_time_delta(r102_id, r105_id, model_names)
                                        compute_time(cod_gest_data, file_fields, time_spent)
                                    else:
                                        file_fields['no_tramitadas'] += 1
                                        file_fields['totals'] += 1


                    ## Tractem els ATCs del subtipus que escau
                    else:
                        subtipus_ids = o.GiscedataSubtipusReclamacio.search([('name', 'in', ['008', '009', '028'])])
                        search_params = [
                            ('create_date', '>=', year_start),
                            ('create_date', '<=', year_end),
                            ('state', '!=', 'cancel'),
                            ('subtipus_id', 'in', subtipus_ids)
                        ]
                        atc_ids = o.GiscedataAtc.search(search_params)
                        total_ts = 0
                        for atc_id in atc_ids:
                            crm_data = o.GiscedataAtc.read(atc_id, ['crm_id', 'state'])
                            if 'done' in crm_data['state']:
                                time_spent = self.get_atc_time_delta(crm_data['crm_id'][0], total_ts, context={})
                                compute_time(cod_gest_data, file_fields, time_spent)
                            else:
                                file_fields['no_tramitadas'] += 1
                                file_fields['totals'] += 1

                ## Tractament general de ATCs
                else:
                    search_params_atc = [
                        ('create_date', '>=', year_start),
                        ('create_date', '<=', year_end),
                        ('cod_gestion_id', '=', item),
                        ('state', '!=', 'cancel')
                    ]
                    atc_ids = o.GiscedataAtc.search(search_params_atc)
                    cod_gest_data = o.GiscedataCodigosGestionCalidadZ.read(item, ['dies_limit', 'name'])
                    total_ts = 0
                    for atc_id in atc_ids:
                        crm_data = o.GiscedataAtc.read(atc_id, ['crm_id', 'state'])
                        if 'done' in crm_data['state']:
                            if 'Z8_01' in cod_gest_data['name']:
                                time_spent = self.get_atc_time_delta(crm_data['crm_id'][0], total_ts, context={})
                                compute_time(cod_gest_data, z8_fields, time_spent)
                            else:
                                time_spent = self.get_atc_time_delta(crm_data['crm_id'][0], total_ts, context={})
                                compute_time(cod_gest_data, file_fields, time_spent)
                        else:
                            file_fields['no_tramitadas'] += 1
                            file_fields['totals'] += 1


                ## Asignem els valors segons escau
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
