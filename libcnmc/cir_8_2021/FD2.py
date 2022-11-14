#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Informe de la CNMC relatiu a la generació conectada de les xarxes de distribució
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
from libcnmc.utils import parse_geom, get_tipus_connexio, format_f, get_ine, convert_srid, get_srid
from shapely import wkt

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

    def consumer(self):

        o = self.connection

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                dentro_plazo = 0
                fuera_plazo = 0
                no_tramitadas = 0
                totals = 0
                year_start = '01-01-' + str(self.year)
                year_end = '12-31-' + str(self.year)
                search_params_atc = [
                    ('create_date', '>=', year_start),
                    ('create_date', '<=', year_end),
                    ('cod_gestion_id', '=', item)
                ]
                atc_ids = o.GiscedataAtc.search(search_params_atc)
                totals = len(atc_ids)
                for atc_id in atc_ids:
                    crm_id = o.GiscedataAtc.read(atc_id, ['crm_id'])['crm_id'][0]
                    crm_state = o.CrmCase.read(crm_id, ['state'])
                    if crm_state is 'done':
                        history_logs = o.CrmCase.read(['history_line'])
                        total_ts = 0
                        for history_log in history_logs:
                            tt_id = o.CrmCaseHistory.read(history_log, ['time_tracking_id'])
                            if tt_id and tt_id[1] == 'Distribuidora':
                                time_spent = o.CrmCaseHistory.read(history_log, ['time_spent'])
                                total_ts = total_ts + time_spent
                        expected_time = o.GiscedataCodigosGestionCalidadZ.read(item, ['dies_limit'])
                        if total_ts > expected_time:
                            fuera_plazo = fuera_plazo + 1
                        else:
                            dentro_plazo = dentro_plazo + 1
                    else:
                        no_tramitadas = no_tramitadas + 1

                output = [
                    item,
                    totals,
                    dentro_plazo,
                    fuera_plazo,
                    no_tramitadas
                ]
                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
