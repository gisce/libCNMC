#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Informe de la CNMC relatiu a la generació conectada de les xarxes de distribució
"""
from __future__ import absolute_import
from datetime import datetime, timedelta
import traceback
from libcnmc.core import StopMultiprocessBased
from workalendar.europe import Spain

ACCEPTED_STATES = [
    'done',
    'pending'
]


class FD2(StopMultiprocessBased):

    def __init__(self, **kwargs):
        super(FD2, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1') or ''
        self.year = kwargs.pop('year', datetime.now().year - 2)
        self.report_name = 'FD2 - Calidad Comercial'
        self.base_object = 'Códigos de gestión de Calidad'

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

    def create_logs(self, create_vals, ref):
        """
            Crea un Log amb les dades adients sobre el cas que estem procesant.
            :params:
                - create_vals: Diccionari amb els valors per crear els logs.
                - ref: Reference que asigname al castracking per saber quin cas el genera.
            :return:
                True
        """
        o = self.connection
        track_obj = o.model('giscedata.circular.82021.case.tracking')
        create_vals.update({'ref': '{},{}'.format(ref[0], ref[1])})
        track_obj.create(create_vals)

        return True

    def compute_time(self, cod_gest_data, values, time_delta, ref):
        """
            Actualitza els valors de la z actuals amb els resultats de el cas que estem procesant.
            També es crea un log per deixa-ho registrat.
            :params:
                - cod_gest_data: Diccionari amb els valors que fem servir del codi de gestió adient.
                - values: Diccionari amb els valors a informar sobre el codi de gestió adient.
                - time_delta: temps que compararem amb el que determina el codi de
                              gestió per determinar si esta fora de plaç o no.
                - ref: Reference que asignem al casetracking per saber quin cas el genera.
            :return:
                True
        """

        o = self.connection
        # Comprovem si tenim el modul de logs
        track_obj_installed = o.IrModel.search([('model', '=', 'giscedata.circular.82021.case.tracking')])
        create_vals = {'cod_gestio_id': cod_gest_data['name']}

        # Si el temps que hem tardat en atendre la sol·licitud
        # es mes gran que:
        #       - Si la Z te data_post_acceptacio, es compara amb aquesta, si no, amb els dies_limit.
        # Altrament dins de plaç.

        dies_limit = cod_gest_data['dies_limit']
        if cod_gest_data['dies_post_acceptacio'] > 0:
            dies_limit = cod_gest_data['dies_post_acceptacio']

        if time_delta <= 0:
            time_delta = 0

        if time_delta > dies_limit:
            values['fuera_plazo'] = values['fuera_plazo'] + 1
            if track_obj_installed:
                create_vals.update({'on_time': True, 'atesa': True})
                self.create_logs(create_vals, ref)
        else:
            values['dentro_plazo'] = values['dentro_plazo'] + 1
            if track_obj_installed:
                create_vals.update({'on_time': False, 'atesa': True})
                self.create_logs(create_vals, ref)

        # Afegim el cas als totals.
        values['totals'] = values['totals'] + 1

        return True

    def get_atc_time_delta(self, crm_id, total_ts, context=None):
        """
            Calcula el temps que s'ha tardat en gestionar la sol·licitud vinculada al ATC que estem processant
            :params:
                - crm_id: id del crm que estem processant.
                - total_ts: Valor del temps que hem tardat en gestionar la sol·licitud del ATC que estem processant.
            :return:
                total_ts actualitzat amb el time spent en el cas que estem processant.
        """

        if context is None:
            context = {}
        o = self.connection

        # Per defecte agafem tots els log del crm que estem tractant
        logs_ids = o.CrmCaseLog.search([('case_id', '=', crm_id)])

        # Si els dies_post_acceptacio != 0 només agafem els logs anteriors al log que determina la data acceptacio.
        dies_acceptacio = context.get('dies_post_acceptacio', False)
        if dies_acceptacio:
            accept_log_id = o.CrmCaseLog.search([('case_id', '=', crm_id), ('name', 'like', '%ATC')])
            if accept_log_id:
                log_date = o.CrmCaseLog.read(accept_log_id, ['date'])['date']
                logs_ids = o.CrmCaseLog.search([('case_id', '=', crm_id), ('date', '<=', log_date)])

        # Sumem el temps que registra cada log sempre que sigui de imputació distribuidora
        for logs_id in logs_ids:
            tt_id = o.CrmCaseLog.read(logs_id, ['time_tracking_id'])['time_tracking_id']
            if tt_id and tt_id[1] == 'Distribuidora':
                time_spent = o.CrmCaseLog.read(logs_id, ['time_spent'])['time_spent']
                total_ts = total_ts + time_spent

        return total_ts

    def get_time_delta(self, start_id, end_id, context=None):
        """
            Calcula el temps que s'ha tardat en processar la sol·licitud ATR que estem processant.
            :params:
                - start_id: id del pas inicial del cas ATR que estem processant.
                - end_id: id del pas final del cas ATR que estem processant.
                - context:
                        - 'model_names': tupla que conté quins son els models de dades
                        tant del pas inical com final.
                        - 'field_names': tupla que conté els camps inicials i finals
                        del model de dades que estipula 'model_names'.
            :return:
                Dies hàbils empleats en atendre el procés ATR que estem processant.
        """

        o = self.connection
        if context is None:
            context = {}

        model_names = context.get('model_names', False)
        field_names = context.get('field_names', False)

        raw_date_start = o.model(model_names[0]).read(start_id, [field_names[0]])[field_names[0]]
        date_start = datetime.strptime(raw_date_start.split(' ')[0], "%Y-%m-%d")

        raw_date_end = o.model(model_names[1]).read(end_id, [field_names[1]])[field_names[1]]
        date_end = datetime.strptime(raw_date_end.split(' ')[0], "%Y-%m-%d")

        return Spain().get_working_days_delta(date_start, date_end)

    def manage_switching_cases(self, cod_gest_data, file_fields, sw_id, start_step_id, context=None):
        """
            Emplena les dades sobre el cas ATR que estem processant. ( Fora de plaç/Dins del Plaç/ No tramitada)
            :params:
                - cod_gest_data: llista amb dades sobre el codi de gestió actual.
                - file_fields: diccionari amb dades sobre els resultats del codi de gestió actual.
                - sw_id: id del cas ATR que estem processant.
                - start_case_id: id del pas inicial del cas ATR que estem processant.
                - context:
                        - 'model_names': tupla que conté quins son els models de dades
                        tant del pas inical com final.
                        - 'field_names': tupla que conté els camps inicials i finals
                        del model de dades que estipula 'model_names'.
            :return:
                True
        """

        o = self.connection

        # Definim els models a fer servir, trobem el cas ATR i la id del pas fins al que contarem els dies.
        model_names = context.get('model_names', False)
        start_step_o = o.model(model_names[0])
        end_step_o = o.model(model_names[1])
        switching_case = model_names[0][20:22]
        end_step_id = end_step_o.search([('sw_id', '=', sw_id)])

        # Si el pas final esta en enviament pendent, marquem com a no tramitat
        # Altrament obtenim el temps empleat al tramit i emplenem amb els valors adients segons el mateix.
        #       si el cas es un B1 actualitzem la id amb la adient segons el model_names
        #       (aixo nomes ho fem amb el B1 ja que no es compta desde el mateix b1 sino des de el R1)
        #
        if end_step_id:
            end_case_id = end_step_id[0]
            enviament_pendent = end_step_o.read(end_case_id, ['enviament_pendent'])['enviament_pendent']
            if not enviament_pendent:
                if switching_case == 'b1':
                    start_step_id = start_step_o.search([('sw_id', '=', sw_id)])[0]
                time_spent = self.get_time_delta(start_step_id, end_case_id, context=context)
                if isinstance(sw_id, list):
                    sw_id = sw_id[0]
                ref = ('giscedata.switching', sw_id)
                self.compute_time(cod_gest_data, file_fields, time_spent, ref)
            else:
                file_fields['no_tramitadas'] += 1
                file_fields['totals'] += 1

        return True

    def process_atcs(self, item, cod_gest_data, file_fields, year_start, year_end, context=None):
        """
            Emplena les dades sobre el cas ATC que estem processant. ( Fora de plaç/Dins del Plaç/ No tramitada)
            :params:
                - item: id de el codi de gestió actual.
                - cod_gest_data: llista amb dades sobre el codi de gestió actual.
                - file_fields: diccionari amb dades sobre els resultats del codi de gestió actual.
                - year_start: data del principi d'any en format string.
                - year_end: data del final d'any en format string.
                - context:
                        - 'subtypes': llista amb les ids dels subtipus a buscar.
            :return:
                True
        """

        o = self.connection
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
        atc_o = o.GiscedataAtc
        # Busquem tots els ATC amb el codi de gestió adient.
        # Si ens ve en el context subtypes, busquem tots els atcs amb els subtipus indicats.
        atc_ids = atc_o.search(search_params)
        total_ts = 0

        # Per cada ATC a tractar:
        #     Si l'estat del ATC no esta reconegut com a tramitat marquem com a no tramitat
        #     Altrament calculem el temps empleat en tramitar i determinem si ha quedat dins o fora de plaç
        for atc_id in atc_ids:
            crm_data = atc_o.read(atc_id, ['crm_id', 'state'])
            ref = ('giscedata.atc', atc_id)
            if crm_data['state'] in ACCEPTED_STATES:
                ctx = {'dies_post_acceptacio': cod_gest_data['dies_post_acceptacio']}
                time_spent = self.get_atc_time_delta(crm_data['crm_id'][0], total_ts, context=ctx)
                if isinstance(atc_id, list):
                    atc_id = atc_id[0]
                self.compute_time(cod_gest_data, file_fields, time_spent, ref)
            else:
                create_vals = {'cod_gestio_id': cod_gest_data['name']}
                self.create_logs(create_vals, ref)
                file_fields['no_tramitadas'] += 1
                file_fields['totals'] += 1

        return True

    def process_z3(self, item, cod_gest_data, file_fields, year_start, year_end):
        """
            Processa les sol·licituds a tenir en compte segons el codi de gestió Z3
            :params:
                - item: id del codi de gestió actual.
                - cod_gest_data: llista amb dades sobre el codi de gestió actual.
                - file_fields: diccionari amb dades sobre els resultats del codi de gestió actual.
                - year_start: data del principi d'any en format string.
                - year_end: data del final d'any en format string.
            :return:
                True
        """

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
                    context = {'model_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.05'],
                               'field_names': ['date_created', 'data_activacio']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)
                elif '03' in proces_name:
                    context = {'model_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.03'],
                               'field_names': ['date_created', 'date_created']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)

        ## Tractem els de alta tensió
        elif '02' in cod_gest_data['name']:
            for at_id in at_ids:
                a302_id = at_id[1]
                sw_id = at_id[0]
                step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                if '05' in proces_name:
                    context = {'model_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.05'],
                               'field_names': ['date_created', 'date_created']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)
                elif '03' in proces_name:
                    context = {'model_names': ['giscedata.switching.a3.02', 'giscedata.switching.a3.03'],
                               'field_names': ['date_created', 'date_created']}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, a302_id, context=context)

        ## Tractem els atcs adients
        self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end)

        return True

    def process_z4(self, item, cod_gest_data, file_fields, year_start, year_end):
        """
            Processa les sol·licituds a tenir en compte segons el codi de gestió Z4
            :params:
                - item: id del codi de gestió actual.
                - cod_gest_data: llista amb dades sobre el codi de gestió actual.
                - file_fields: diccionari amb dades sobre els resultats del codi de gestió actual.
                - year_start: data del principi d'any en format string.
                - year_end: data del final d'any en format string.
            :return:
                True
        """
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

        return True

    def process_z5(self, item, cod_gest_data, file_fields, year_start, year_end, context=None):
        """
            Processa les sol·licituds a tenir en compte segons el codi de gestió Z5
            :params:
                - item: id del codi de gestió actual.
                - cod_gest_data: llista amb dades sobre el codi de gestió actual.
                - file_fields: diccionari amb dades sobre els resultats del codi de gestió actual.
                - year_start: data del principi d'any en format string.
                - year_end: data del final d'any en format string.
            :return:
                True
        """
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
            b104_id = o.model('giscedata.switching.b1.04').search([('sw_id', '=', sw_id)])
            data_rebuig_04 = o.model('giscedata.switching.b1.04').read(b104_id, ['data_rebuig'])['data_rebuig']
            if b101_id and not data_rebuig_04:
                b101_id = b101_id[0]
                motiu_b1 = o.model('giscedata.switching.b1.01').read(b101_id, ['motiu'])['motiu']
                if motiu_b1 == '03':
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, b103_id, context=context)

        ## Tractem els atcs que escau
        self.process_atcs(item, cod_gest_data, file_fields, year_start, year_end, context=context)

    def process_z6(self, item, cod_gest_data, file_fields, year_start, year_end):
        """
            Processa les sol·licituds a tenir en compte segons el codi de gestió Z6
            :params:
                - item: id del codi de gestió actual.
                - cod_gest_data: llista amb dades sobre el codi de gestió actual.
                - file_fields: diccionari amb dades sobre els resultats del codi de gestió actual.
                - year_start: data del principi d'any en format string.
                - year_end: data del final d'any en format string.
            :return:
                True
        """
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

        return True

    def process_z7(self, item, cod_gest_data, file_fields, year_start, year_end):
        """
            Processa les sol·licituds a tenir en compte segons el codi de gestió Z7
            :params:
                - item: id del codi de gestió actual.
                - cod_gest_data: llista amb dades sobre el codi de gestió actual.
                - file_fields: diccionari amb dades sobre els resultats del codi de gestió actual.
                - year_start: data del principi d'any en format string.
                - year_end: data del final d'any en format string.
            :return:
                True
        """

        o = self.connection

        if '01' in cod_gest_data['name']:
            search_params = [
                ('date_created', '>=', year_start),
                ('date_created', '<=', year_end)
            ]
            c101_ids = o.model("giscedata.switching.c1.01").search(search_params)

            ## Tractem els c1 i comptabilitzem els que escau
            for c101_id in c101_ids:
                c1_header_id = o.model("giscedata.switching.c1.01").read(c101_id, ['header_id'])[
                    'header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(c1_header_id[0], ['sw_id'])['sw_id'][0]
                step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                if '05' in proces_name:
                    model_names = ['giscedata.switching.c1.01', 'giscedata.switching.c1.05']
                    field_names = ['date_created', 'data_activacio']
                    context = {'model_names': model_names, 'field_names': field_names}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, c101_id, context=context)

            c201_ids = o.model("giscedata.switching.c2.01").search(search_params)

            ## Tractem els c2 i comptabilitzem els que escau
            for c201_id in c201_ids:
                c2_header_id = o.model("giscedata.switching.c2.01").read(c201_id, ['header_id'])[
                    'header_id']
                sw_id = o.GiscedataSwitchingStepHeader.read(c2_header_id[0], ['sw_id'])['sw_id'][0]
                step_id = o.GiscedataSwitching.read(sw_id, ['step_id'])['step_id'][0]
                proces_name = o.model('giscedata.switching.step').read(step_id, ['name'])['name']
                if '05' in proces_name:
                    model_names = ['giscedata.switching.c2.01', 'giscedata.switching.c2.05']
                    field_names = ['date_created', 'data_activacio']
                    context = {'model_names': model_names, 'field_names': field_names}
                    self.manage_switching_cases(cod_gest_data, file_fields, sw_id, c201_id, context=context)

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
                    polissa_id = o.GiscedataSwitching.read(sw_id, ['cups_polissa_id'])['cups_polissa_id'][0]
                    data_act = o.model('giscedata.switching.c1.05').read(c105_id, ['data_activacio'])['data_activacio']
                    data_act = datetime.strftime(datetime.strptime(data_act, "%Y-%m-%d") - timedelta(1), "%Y-%m-%d")
                    fact_id = o.GiscedataPolissa.get_last_invoice_by_partner(polissa_id, comer_sortint_id,
                                                                             {'data_act': data_act})
                    if not fact_id:
                        continue
                    invoice_id = o.GiscedataFacturacioFactura.read(fact_id, ['invoice_id'])['invoice_id'][0]
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
                    polissa_id = o.GiscedataSwitching.read(sw_id, ['cups_polissa_id'])['cups_polissa_id'][0]
                    data_act = o.model('giscedata.switching.c2.05').read(c205_id, ['data_activacio'])['data_activacio']
                    data_act = datetime.strftime(datetime.strptime(data_act, "%Y-%m-%d") - timedelta(1), "%Y-%m-%d")
                    fact_id = o.GiscedataPolissa.get_last_invoice_by_partner(polissa_id, comer_sortint_id,
                                                                             {'data_act': data_act})
                    if not fact_id:
                        continue
                    invoice_id = o.GiscedataFacturacioFactura.read(fact_id, ['invoice_id'])['invoice_id'][0]
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

        return True

    def process_z8(self, item, cod_gest_data, file_fields, year_start, year_end):
        """
            Processa les sol·licituds a tenir en compte segons el codi de gestió Z8
            :params:
                - item: id del codi de gestió actual.
                - cod_gest_data: llista amb dades sobre el codi de gestió actual.
                - file_fields: diccionari amb dades sobre els resultats del codi de gestió actual.
                - year_start: data del principi d'any en format string.
                - year_end: data del final d'any en format string.
            :return:
                True
        """
        o = self.connection

        if '08' in cod_gest_data['name']:
            # Obtenim els R1 amb els subtipus adients i els tractem.
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

        # Obtenim els R1 amb els subtipus adients i els tractem.
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

        # Obtenim els ATR del tipus adient amb treball de camp i els tractem
        elif '10' in cod_gest_data['name']:
            search_params = [
                ('date_created', '>=', year_start),
                ('date_created', '<=', year_end),
                ('actuacio_camp', '=', 'S')
            ]
            model_names = ['giscedata.switching.a3.02', 'giscedata.switching.a3.05']
            field_names = ['date_created', 'data_activacio']
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
            field_names = ['date_created', 'data_activacio']
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
            field_names = ['date_created', 'data_activacio']
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

        return True

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
                cod_gest_data = o.GiscedataCodigosGestionCalidadZ.read(item, ['dies_limit', 'name', 'id',
                                                                              'dies_post_acceptacio'])

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
                    cod_gest_data = o.GiscedataCodigosGestionCalidadZ.read(item, ['dies_limit', 'name', 'id',
                                                                                  'dies_post_acceptacio'])
                    total_ts = 0
                    for atc_id in atc_ids:
                        crm_data = o.GiscedataAtc.read(atc_id, ['crm_id', 'state'])
                        if 'done' in crm_data['state']:
                            if isinstance(atc_id, list):
                                atc_id = atc_id[0]
                            if 'Z8_01' in cod_gest_data['name']:
                                ctx = {'dies_post_acceptacio': cod_gest_data['dies_post_acceptacio']}
                                time_spent = self.get_atc_time_delta(crm_data['crm_id'][0], total_ts, context=ctx)
                                ref = ('giscedata.atc', atc_id)
                                self.compute_time(cod_gest_data, file_fields, time_spent, ref)
                            else:
                                ctx = {'dies_post_acceptacio': cod_gest_data['dies_post_acceptacio']}
                                time_spent = self.get_atc_time_delta(crm_data['crm_id'][0], total_ts, context=ctx)
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
