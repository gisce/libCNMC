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

    def manage_switching_cases(self, cod_gest_data, file_fields, sw_id, case_id, context=None):

        o = self.connection
        model_names = context.get('model_names', False)
        end_case_o = o.model(model_names[1])
        switching_type = model_names[0][20:22]

        end_case_id = end_case_o.search([('sw_id', '=', sw_id)])[0]
        if end_case_id:
            enviament_pendent = end_case_o.read(end_case_id, ['enviament_pendent'])['enviament_pendent']
            if not enviament_pendent:
                if switching_type != 'r1':
                    method = "get_{}_time_delta".format(model_names[0][20:22])
                else:
                    method = "get_time_delta"
                time_spent = getattr(self, method)(case_id, end_case_id, context=context)
                compute_time(cod_gest_data, file_fields, time_spent)
            file_fields['no_tramitadas'] += 1
            file_fields['totals'] += 1


    def process_atcs(self, item, cod_gest_data, file_fields, year_start, year_end, context=None):
        o = self.connection
        atc_o = o.GiscedataAtc
        if context is None:
            context = {}
        search_params = [
            ('create_date', '>=', year_start),
            ('create_date', '<=', year_end),
            ('state', '!=', 'cancel'),
            ('cod_gestion_id', '=', item)
        ]
        subtypes = context.get('subtypes', False)
        if subtypes:
            search_params[3] = ('subtipus_id', 'in', subtypes)

        atc_ids = atc_o.search(search_params)
        total_ts = 0
        for atc_id in atc_ids:
            crm_data = atc_o.read(atc_id, ['crm_id', 'state'])
            if 'done' in crm_data['state']:
                time_spent = self.get_atc_time_delta(crm_data['crm_id'][0], total_ts, context={})
                compute_time(cod_gest_data, file_fields, time_spent)
            else:
                file_fields['no_tramitadas'] += 1
                file_fields['totals'] += 1

    def process_z3(self, item, cod_gest_data, file_fields, year_start, year_end):

        o = self.connection

        search_params = [
            ('date_created', '>=', year_start),
            ('date_created', '<=', year_end)
        ]
        a302_ids = o.model("giscedata.switching.a3.02").search(search_params)

        bt_ids = []
        at_ids = []

        ## Separem els a3 de baixa i alta tensió per a tractarlos per separat
        for a302_id in a302_ids:
            a3_header_id = o.model("giscedata.switching.a3.02").read(a302_id, ['header_id'])[
                'header_id']
            sw_id = o.GiscedataSwitchingStepHeader.read(a3_header_id[0], ['sw_id'])['sw_id'][0]
            pol_id = o.GiscedataSwitching.read(sw_id, ['cups_polissa_id'])['cups_polissa_id'][0]
            t_norm = o.GiscedataPolissa.read(pol_id, ['tensio_normalitzada'])['tensio_normalitzada'][0]
            t_tipus = o.GiscedataTensionsTensio.read(t_norm, ['tipus'])['tipus']
            if t_tipus is 'AT':
                at_ids.append((sw_id, a302_id))
            else:
                bt_ids.append((sw_id, a302_id))

        ## Tractem els de baixa tensió
        if '01' in cod_gest_data['name']:
            for bt_id in bt_ids:
                a302_id = bt_id[1]
                sw_id = bt_id[0]
                step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                if '05' in proces_name:
                    context = {'method_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.05']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)
                elif '03' in proces_name:
                    context = {'method_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.03']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)

        ## Tractem els de alta tensió
        if '02' in cod_gest_data['name']:
            for at_id in at_ids:
                a302_id = at_id[1]
                sw_id = at_id[0]
                step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                if '05' in proces_name:
                    context = {'method_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.05']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)
                elif '03' in proces_name:
                    context = {'method_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.03']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)

        ## Tractem els atcs adients
        self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end)

    def process_z4(self, item, cod_gest_data, file_fields, year_start, year_end):
        o = self.connection
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
                    model_names = ['giscedata.switching.r1.02', 'giscedata.switching.r1.05']
                    field_names = ['date_created', 'date_created']
                    context = {'model_names': model_names, 'field_names': field_names}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, r102_id, context=context)

            ## Tractem els atcs adients
            self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end)

        ## Tractem els ATCs del subtipus que escau
        else:
            subtipus_ids = o.GiscedataSubtipusReclamacio.search([('name', 'in', ['008', '009', '028'])])
            context = {'subtype': subtipus_ids}
            self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end, context=context)

    def process_z5(self, item, cod_gest_data, file_fields, year_start, year_end):

        o = self.connection

        search_params = [
            ('date_created', '>=', year_start),
            ('date_created', '<=', year_end),
            ('motiu', '=', '03')
        ]
        model_names = ['giscedata.switching.b1.03', 'giscedata.switching.b1.04']
        field_names = ['create_date', 'create_date']
        context = {'model_names': model_names, 'field_names': field_names}
        b101_ids = o.model("giscedata.switching.b1.01").search(search_params)
        for b101_id in b101_ids:
            b1_header_id = o.model("giscedata.switching.b1.01").read(b101_id, ['header_id'])['header_id']
            sw_id = o.GiscedataSwitchingStepHeader.read(b1_header_id[0], ['sw_id'])['sw_id'][0]
            self.manage_switching_cases(cod_gest_data, file_fields, sw_id, b101_id, context=context)

        ## Tractem els atcs que escau
        self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end, context=context)

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

    def get_time_delta(self, start_id, end_id, context=None):

        o = self.connection
        if context is None:
            context = {}

        model_names = context.get('model_names', False)
        field_names = context.get('field_names', False)


        raw_date_end = o.model(model_names[1]).read(end_id, field_names[1])[field_names[1]]
        raw_date_start = o.model(model_names[0]).read(start_id, field_names[0])[field_names[0]]
        date_end = datetime.strptime(raw_date_end.split(' ')[0], "%Y-%m-%d")
        date_start = datetime.strptime(raw_date_start.split(' ')[0], "%Y-%m-%d")

        return Spain().get_working_days_delta(date_start, date_end)

    def get_a3_time_delta(self, start_id, end_id, context=None):

        o = self.connection
        model_name = context.get('model_names', False)
        if '05' in model_name[0]:
            raw_date_end = o.model(model_name[0]).read(end_id, ['data_activacio'])['data_activacio']
        else:
            raw_date_end = o.model(model_name[0]).read(end_id, ['date_created'])['date_created']

        raw_date_start = o.model(model_name[1]).read(start_id, ['date_created'])['date_created']
        date_end = datetime.strptime(raw_date_end.split(' ')[0], "%Y-%m-%d")
        date_start = datetime.strptime(raw_date_start.split(' ')[0], "%Y-%m-%d")

        return Spain().get_working_days_delta(date_start, date_end)

    def get_b1_time_delta(self, start_id, end_id, context=None):

        o = self.connection
        model_name = context.get('model_names', False)
        if '05' in model_name[0]:
            raw_date_end = o.model(model_name[0]).read(end_id, ['data_activacio'])['data_activacio']
        else:
            raw_date_end = o.model(model_name[0]).read(end_id, ['date_created'])['date_created']

        raw_date_start = o.model(model_name[1]).read(start_id, ['date_created'])['date_created']
        date_end = datetime.strptime(raw_date_end.split(' ')[0], "%Y-%m-%d")
        date_start = datetime.strptime(raw_date_start.split(' ')[0], "%Y-%m-%d")

        return Spain().get_working_days_delta(date_start, date_end)

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
                    self.process_z4(item, cod_gest_data, file_fields, year_start, year_end)
                elif 'Z3' in cod_gest_data['name']:
                    self.process_z3(item, cod_gest_data, file_fields, year_start, year_end)
                elif 'Z5' in cod_gest_data['name']:
                    self.process_z5(item, cod_gest_data, file_fields, year_start, year_end)
                elif 'Z6' in cod_gest_data['name']:
                    pass
                elif 'Z7' in cod_gest_data['name']:
                    pass
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
