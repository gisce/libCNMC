#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Informe de la CNMC relatiu a la generació conectada de les xarxes de distribució
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import StopMultiprocessBased
from workalendar.europe import Spain



class FD2(StopMultiprocessBased):

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
            '|',
            ('name', 'like', '(intern)'),
            ('name', 'like', 'Z8_01_dl5')
        ]
        intern_z = self.connection.GiscedataCodigosGestionCalidadZ.search(search_params_interns)
        search_params = [
            ('id', 'not in', intern_z)
        ]
        return self.connection.GiscedataCodigosGestionCalidadZ.search(search_params)

    def compute_time(self, cod_gest_data, values, time_delta, ref):
        o = self.connection
        create_vals = {'cod_gestio_id': cod_gest_data['id'], 'ref': '{},{}'.format(ref[0], ref[1])}
        track_obj_installed = o.IrModel.search([('model', '=', 'giscedata.circular.82021.case.tracking')])
        track_obj = o.model('giscedata.circular.82021.case.tracking')
        if time_delta > cod_gest_data['dies_limit']:
            values['fuera_plazo'] = values['fuera_plazo'] + 1
            if track_obj_installed:
                create_vals.update({'on_time': False})
                track_obj.create_async(create_vals)
        else:
            values['dentro_plazo'] = values['dentro_plazo'] + 1
            if track_obj_installed:
                create_vals.update({'on_time': True})
                track_obj.create_async(create_vals)
        values['totals'] = values['totals'] + 1

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

        raw_date_end = o.model(model_names[1]).read(end_id, [field_names[1]])[field_names[1]]
        raw_date_start = o.model(model_names[0]).read(start_id, [field_names[0]])[field_names[0]]
        date_end = datetime.strptime(raw_date_end.split(' ')[0], "%Y-%m-%d")
        date_start = datetime.strptime(raw_date_start.split(' ')[0], "%Y-%m-%d")

        return Spain().get_working_days_delta(date_start, date_end)

    def get_a3_time_delta(self, start_id, end_id, context=None):

        o = self.connection
        model_name = context.get('model_names', False)
        if '05' in model_name[1]:
            raw_date_end = o.model(model_name[1]).read(end_id, ['data_activacio'])['data_activacio']
        else:
            raw_date_end = o.model(model_name[1]).read(end_id, ['date_created'])['date_created']

        raw_date_start = o.model(model_name[0]).read(start_id, ['date_created'])['date_created']
        date_end = datetime.strptime(raw_date_end.split(' ')[0], "%Y-%m-%d")
        date_start = datetime.strptime(raw_date_start.split(' ')[0], "%Y-%m-%d")

        return Spain().get_working_days_delta(date_start, date_end)

    def get_b1_time_delta(self, start_id, end_id, context=None):

        o = self.connection
        model_name = context.get('model_names', False)
        if '05' in model_name[1]:
            raw_date_end = o.model(model_name[1]).read(end_id, ['data_activacio'])['data_activacio']
        else:
            raw_date_end = o.model(model_name[1]).read(end_id, ['date_created'])['date_created']

        raw_date_start = o.model(model_name[0]).read(start_id, ['date_created'])['date_created']
        date_end = datetime.strptime(raw_date_end.split(' ')[0], "%Y-%m-%d")
        date_start = datetime.strptime(raw_date_start.split(' ')[0], "%Y-%m-%d")

        return Spain().get_working_days_delta(date_start, date_end)

    def manage_switching_cases(self, cod_gest_data, file_fields, sw_id, start_case_id, context=None):

        o = self.connection
        model_names = context.get('model_names', False)
        start_case_o = o.model(model_names[0])
        end_case_o = o.model(model_names[1])
        switching_type = model_names[0][20:22]
        end_case_id = end_case_o.search([('sw_id', '=', sw_id)])
        if end_case_id:
            end_case_id = end_case_id[0]
            enviament_pendent = end_case_o.read(end_case_id, ['enviament_pendent'])['enviament_pendent']
            if not enviament_pendent:
                if switching_type == 'a3':
                    method = "get_{}_time_delta".format(model_names[0][20:22])
                elif switching_type == 'b1':
                    start_case_id = start_case_o.search([('sw_id', '=', sw_id)])[0]
                    method = "get_time_delta"
                else:
                    method = "get_time_delta"
                time_spent = getattr(self, method)(start_case_id, end_case_id, context=context)
                if isinstance(sw_id, list):
                    sw_id = sw_id[0]
                ref = ('giscedata.switching', sw_id)
                self.compute_time(cod_gest_data, file_fields, time_spent, ref)
            else:
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
                if isinstance(atc_id, list):
                    atc_id = atc_id[0]
                ref = ('giscedata.atc', atc_id)
                self.compute_time(cod_gest_data, file_fields, time_spent, ref)
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
            pol_id = o.GiscedataSwitching.read(sw_id, ['cups_polissa_id'])['cups_polissa_id']
            if pol_id:
                pol_id = pol_id[0]
                t_norm = o.GiscedataPolissa.read(pol_id, ['tensio_normalitzada'])['tensio_normalitzada']
                if t_norm:
                    t_norm = t_norm[0]
                    t_tipus = o.GiscedataTensionsTensio.read(t_norm, ['tipus'])['tipus']
                    if 'AT' in t_tipus:
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
                    context = {'model_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.05']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)
                elif '03' in proces_name:
                    context = {'model_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.03']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)
        ## Tractem els de alta tensió
        elif '02' in cod_gest_data['name']:
            for at_id in at_ids:
                a302_id = at_id[1]
                sw_id = at_id[0]
                step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                if '05' in proces_name:
                    context = {'model_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.05']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)
                elif '03' in proces_name:
                    context = {'model_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.03']}
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

    def process_z5(self, item, cod_gest_data, file_fields, year_start, year_end, context=None):

        o = self.connection

        search_params = [
            ('date_created', '>=', year_start),
            ('date_created', '<=', year_end)
        ]
        model_names = ['giscedata.switching.b1.03', 'giscedata.switching.b1.04']
        field_names = ['date_created', 'data_acceptacio']
        context = {'model_names': model_names, 'field_names': field_names}
        b103_ids = o.model("giscedata.switching.b1.03").search(search_params)
        for b103_id in b103_ids:
            b3_header_id = o.model("giscedata.switching.b1.03").read(b103_id, ['header_id'])['header_id']
            sw_id = o.GiscedataSwitchingStepHeader.read(b3_header_id[0], ['sw_id'])['sw_id'][0]
            b101_id = o.model('giscedata.switching.b1.01').search([('sw_id', '=', sw_id)])
            if b101_id:
                b101_id = b101_id[0]
                motiu_b1 = o.model('giscedata.switching.b1.01').read(b101_id, ['motiu'])['motiu']
                if motiu_b1 == '03':
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, b103_id, context=context)

        ## Tractem els atcs que escau
        self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end, context=context)

    def process_z6(self, item, cod_gest_data, file_fields, year_start, year_end):
        o = self.connection

        subtipus_ids = o.GiscedataSubtipusReclamacio.search([('name', 'in', ['028'])])
        search_params = [
            ('date_created', '>=', year_start),
            ('date_created', '<=', year_end),
            ('subtipus_id', 'in', subtipus_ids)
        ]
        r101_ids = o.model("giscedata.switching.r1.01").search(search_params)

        ## Tractem els r1 i comptabilitzem els que escau
        for r101_id in r101_ids:
            r1_header_id = o.model("giscedata.switching.r1.02").read(r101_id, ['header_id'])[
                'header_id']
            sw_id = o.GiscedataSwitchingStepHeader.read(r1_header_id[0], ['sw_id'])['sw_id'][0]
            pol_id = o.GiscedataSwitching.read(sw_id, ['cups_polissa_id'])['cups_polissa_id'][0]
            b101_ids = o.model("giscedata.switching.b1.01").search([('sw_id', '=', sw_id), ('motiu', '=', '03')])
            for b101_id in b101_ids:
                b1_header_id = o.model("giscedata.switching.b1.01").read(r101_id, ['header_id'])[
                    'header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(b1_header_id[0], ['sw_id'])['sw_id'][0]
                b1_pol_id = o.GiscedataSwitchin.read(b101_id, ['cups_polissa_id'])['cups_polissa_id'][0]
                if b1_pol_id == pol_id:
                    b103_id = o.model('giscedata.switching.b1.03').search([('sw_id', '=', sw_id)])
                    model_names = ['giscedata.switching.r1.03', 'giscedata.switching.r1.04']
                    field_names = ['date_created', 'data_activacio']
                    context = {'model_names': model_names, 'field_names': field_names}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, b103_id, context=context)

        ## Tractem els atcs adients
        self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end)
    
    def process_z7(self, item, cod_gest_data, file_fields, year_start, year_end):
        o = self.connection
        
        if '01' in cod_gest_data['name']:
            search_params = [
                ('date_created', '>=', year_start),
                ('date_created', '<=', year_end)
            ]
            c102_ids = o.model("giscedata.switching.c1.01").search(search_params)

            ## Tractem els c1 i comptabilitzem els que escau
            for c102_id in c102_ids:
                c1_header_id = o.model("giscedata.switching.c1.01").read(c102_id, ['header_id'])[
                    'header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(c1_header_id[0], ['sw_id'])['sw_id'][0]
                step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                if '05' in proces_name:
                    model_names = ['giscedata.switching.c1.01', 'giscedata.switching.c1.05']
                    field_names = ['date_created', 'data_activacio']
                    context = {'model_names': model_names, 'field_names': field_names}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, c102_id, context=context)

            c202_ids = o.model("giscedata.switching.c2.01").search(search_params)

            ## Tractem els c2 i comptabilitzem els que escau
            for c202_id in c202_ids:
                c2_header_id = o.model("giscedata.switching.c2.01").read(c202_id, ['header_id'])[
                    'header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(c2_header_id[0], ['sw_id'])['sw_id'][0]
                step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                if '05' in proces_name:
                    model_names = ['giscedata.switching.c2.01', 'giscedata.switching.c2.05']
                    field_names = ['date_created', 'data_activacio']
                    context = {'model_names': model_names, 'field_names': field_names}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, c202_id, context=context)

        else:
            search_params = [
                ('date_created', '>=', year_start),
                ('date_created', '<=', year_end)
            ]
            c105_ids = o.model("giscedata.switching.c1.05").search(search_params)

            ## Tractem els c1 i comptabilitzem els que escau
            for c105_id in c105_ids:
                c1_header_id = o.model("giscedata.switching.c1.05").read(c105_id, ['header_id'])[
                    'header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(c1_header_id[0], ['sw_id'])['sw_id'][0]
                step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                if '05' in proces_name:
                    comer_sortint_id = o.GiscedataSwitching.read(sw_id, ['comer_sortint_id'])['comer_sortint_id'][0]
                    invoice_id = o.AccountInvoice.search([('partner_id', '=', comer_sortint_id)], 0, None, 'date_invoice asc')[0]
                    model_names = ['giscedata.switching.c1.05', 'account.invoice']
                    field_names = ['data_activacio', 'date_invoice']
                    context = {'model_names': model_names, 'field_names': field_names}
                    time_spent = self.get_time_delta(c105_id, invoice_id, context=context)
                    if isinstance(sw_id, list):
                        sw_id = sw_id[0]
                    ref = ('giscedata.switching', sw_id)
                    self.compute_time(cod_gest_data, file_fields, time_spent, ref)
                else:
                    file_fields['no_tramitadas'] += 1
                    file_fields['totals'] += 1
                    
            c205_ids = o.model("giscedata.switching.c2.05").search(search_params)

            ## Tractem els c2 i comptabilitzem els que escau
            for c205_id in c205_ids:
                c2_header_id = o.model("giscedata.switching.c2.05").read(c205_id, ['header_id'])[
                    'header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(c2_header_id[0], ['sw_id'])['sw_id'][0]
                step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                if '05' in proces_name:
                    comer_sortint_id = o.GiscedataSwitching.read(sw_id, ['comer_sortint_id'])['comer_sortint_id'][0]
                    invoice_id = o.AccountInvoice.search([('partner_id', '=', comer_sortint_id)], 0, None, 'date_invoice asc', )[0]
                    model_names = ['giscedata.switching.c2.05', 'account.invoice']
                    field_names = ['data_activacio', 'date_invoice']
                    context = {'model_names': model_names, 'field_names': field_names}
                    time_spent = self.get_time_delta(c205_id, invoice_id, context=context)
                    if isinstance(sw_id, list):
                        sw_id = sw_id[0]
                    ref = ('giscedata.switching', sw_id)
                    self.compute_time(cod_gest_data, file_fields, time_spent, ref)
                else:
                    file_fields['no_tramitadas'] += 1

                    file_fields['totals'] += 1

        ## Tractem els atcs adients
        self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end)

    def process_z8(self, item, cod_gest_data, file_fields, year_start, year_end):
        o = self.connection

        if '08' in cod_gest_data['name']:
            subtipus_list = ['058', '059', '060', '061', '062', '064', '064', '071', '072', '074']
            subtipus_ids = o.GiscedataSubtipusReclamacio.search([('name', 'in', subtipus_list)])
            search_params = [
                ('date_created', '>=', year_start),
                ('date_created', '<=', year_end),
                ('subtipus_id', 'in', subtipus_ids)
            ]
            r101_ids = o.model("giscedata.switching.r1.01").search(search_params)

            for r101_id in r101_ids:
                r1_header_id = o.model("giscedata.switching.r1.01").read(r101_id, ['header_id'])[
                    'header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(r1_header_id[0], ['sw_id'])['sw_id'][0]
                r105_id = o.model("giscedata.switching.r1.05").search([('sw_id', '=', sw_id)])
                if r105_id:
                    model_names = ['giscedata.switching.r1.01', 'giscedata.switching.r1.05']
                    field_names = ['date_created', 'date_created']
                    context = {'model_names': model_names, 'field_names': field_names}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, r101_id, context=context)

        elif '09' in cod_gest_data['name']:
            subtipus_list = ['003', '004']
            subtipus_ids = o.GiscedataSubtipusReclamacio.search([('name', 'in', subtipus_list)])
            search_params = [
                ('date_created', '>=', year_start),
                ('date_created', '<=', year_end),
                ('subtipus_id', 'in', subtipus_ids)
            ]
            r101_ids = o.model("giscedata.switching.r1.01").search(search_params)

            for r101_id in r101_ids:
                r1_header_id = o.model("giscedata.switching.r1.01").read(r101_id, ['header_id'])[
                    'header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(r1_header_id[0], ['sw_id'])['sw_id'][0]
                r105_id = o.model("giscedata.switching.r1.05").search([('sw_id', '=', sw_id)])
                if r105_id:
                    model_names = ['giscedata.switching.r1.01', 'giscedata.switching.r1.05']
                    field_names = ['date_created', 'date_created']
                    context = {'model_names': model_names, 'field_names': field_names}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, r101_id, context=context)

        elif '10' in cod_gest_data['name']:
            search_params = [
                ('date_created', '>=', year_start),
                ('date_created', '<=', year_end),
                ('actuacio_camp', '=', 'S')
            ]
            model_names = ['giscedata.switching.a3.02', 'giscedata.switching.a3.05']
            field_names = ['date_created', 'data_acceptacio']
            context = {'model_names': model_names, 'field_names': field_names}
            a302_ids = o.model("giscedata.switching.a3.02").search(search_params)
            for a302_id in a302_ids:
                a3_header_id = o.model("giscedata.switching.a3.02").read(a302_id, ['header_id'])['header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(a3_header_id[0], ['sw_id'])['sw_id'][0]
                self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)

            search_params = [
                ('date_created', '>=', year_start),
                ('date_created', '<=', year_end),
                ('actuacio_camp', '=', 'S')
            ]
            model_names = ['giscedata.switching.m1.02', 'giscedata.switching.m1.05']
            field_names = ['date_created', 'data_acceptacio']
            context = {'model_names': model_names, 'field_names': field_names}
            m102_ids = o.model("giscedata.switching.m1.02").search(search_params)
            for m102_id in m102_ids:
                m1_header_id = o.model("giscedata.switching.m1.02").read(m102_id, ['header_id'])['header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(m1_header_id[0], ['sw_id'])['sw_id'][0]
                self.manage_switching_cases(cod_gest_data, file_fields, sw_id, m102_id, context=context)

            search_params = [
                ('date_created', '>=', year_start),
                ('date_created', '<=', year_end),
                ('actuacio_camp', '=', 'S')
            ]
            model_names = ['giscedata.switching.c2.02', 'giscedata.switching.c2.05']
            field_names = ['date_created', 'data_acceptacio']
            context = {'model_names': model_names, 'field_names': field_names}
            c202_ids = o.model("giscedata.switching.c2.02").search(search_params)
            for c202_id in c202_ids:
                c2_header_id = o.model("giscedata.switching.c2.02").read(c202_id, ['header_id'])['header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(c2_header_id[0], ['sw_id'])['sw_id'][0]
                self.manage_switching_cases(cod_gest_data, file_fields, sw_id, c202_id, context=context)

        if '01' in cod_gest_data['name']:
            self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end)
            z801_dl5_id = o.GiscedataCodigosGestionCalidadZ.search([('name', '=', 'Z8_01_dl5')])
            self.process_atcs(z801_dl5_id, cod_gest_data, file_fields, year_start, year_end)
        else:
            self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end)

    def consumer(self):

        o = self.connection

        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                file_fields = {'totals': 0, 'dentro_plazo': 0, 'fuera_plazo': 0, 'no_tramitadas': 0,
                               'debug_helper': [0, 0]}
                z8_fields = {'totals': 0, 'dentro_plazo': 0, 'fuera_plazo': 0, 'no_tramitadas': 0}
                year_start = '01-01-' + str(self.year)
                year_end = '12-31-' + str(self.year)
                cod_gest_data = o.GiscedataCodigosGestionCalidadZ.read(item, ['dies_limit', 'name', 'id'])

                ## Tractem el codi de gestio Z4
                if 'Z3' in cod_gest_data['name']:
                    self.process_z3(item, cod_gest_data, file_fields, year_start, year_end)
                elif 'Z4' in cod_gest_data['name']:
                    self.process_z4(item, cod_gest_data, file_fields, year_start, year_end)
                elif 'Z5' in cod_gest_data['name']:
                    self.process_z5(item, cod_gest_data, file_fields, year_start, year_end)
                elif 'Z6' in cod_gest_data['name']:
                    self.process_z6(item, cod_gest_data, file_fields, year_start, year_end)
                elif 'Z7' in cod_gest_data['name']:
                    self.process_z7(item, cod_gest_data, file_fields, year_start, year_end)
                elif 'Z8' in cod_gest_data['name']:
                    self.process_z8(item, cod_gest_data, z8_fields, year_start, year_end)

                ## Tractament general de ATCs
                else:
                    search_params_atc = [
                        ('create_date', '>=', year_start),
                        ('create_date', '<=', year_end),
                        ('cod_gestion_id', '=', item),
                        ('state', '!=', 'cancel')
                    ]
                    atc_ids = o.GiscedataAtc.search(search_params_atc)
                    cod_gest_data = o.GiscedataCodigosGestionCalidadZ.read(item, ['dies_limit', 'name', 'id'])
                    total_ts = 0
                    for atc_id in atc_ids:
                        crm_data = o.GiscedataAtc.read(atc_id, ['crm_id', 'state'])
                        if 'done' in crm_data['state']:
                            if isinstance(atc_id, list):
                                atc_id = atc_id[0]
                            if 'Z8_01' in cod_gest_data['name']:
                                time_spent = self.get_atc_time_delta(crm_data['crm_id'][0], total_ts, context={})
                                ref = ('giscedata.atc', atc_id)
                                self.compute_time(cod_gest_data, file_fields, time_spent, ref)
                            else:
                                time_spent = self.get_atc_time_delta(crm_data['crm_id'][0], total_ts, context={})
                                ref = ('giscedata.atc', atc_id)
                                self.compute_time(cod_gest_data, file_fields, time_spent, ref)
                        else:
                            file_fields['no_tramitadas'] += 1
                            file_fields['totals'] += 1

                ## Asignem els valors segons escau
                if 'Z8' not in cod_gest_data['name']:
                    output = [
                        cod_gest_data['name'],
                        file_fields['totals'],
                        file_fields['dentro_plazo'],
                        file_fields['fuera_plazo'],
                        file_fields['no_tramitadas'],
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
                else:
                    output = [
                        cod_gest_data['name'],
                        z8_fields['totals'],
                        z8_fields['dentro_plazo'],
                        z8_fields['fuera_plazo'],
                        z8_fields['no_tramitadas']
                    ]
                    self.output_q.put(output)

                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
