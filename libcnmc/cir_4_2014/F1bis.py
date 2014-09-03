# -*- coding: utf-8 -*-
from datetime import datetime
import traceback

import click
from libcnmc.utils import N_PROC
from libcnmc.core import MultiprocessBased


class F1bis(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F1bis, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F1bis - CUPS'
        self.base_object = 'CUPS'

    def get_sequence(self):
        search_params = [('polissa_polissa', '!=', False)]
        return self.connection.GiscedataCupsPs.search(search_params)

    def get_comptador(self, polissa_id):
        O = self.connection
        comp_obj = O.GiscedataLecturesComptador
        comp_id = comp_obj.search([
            ('polissa', '=', polissa_id),
            ('data_alta', '<', '%s-01-01' % (self.year + 1))
        ], 0, 1, 'data_alta desc', {'active_test': False})
        return comp_id

    def get_comptador_cini(self, polissa_id):
        comp_obj = self.connection.GiscedataLecturesComptador
        cid = self.get_comptador(polissa_id)
        if cid:
            comptador = comp_obj.read(cid[0], ['cini'])
            cini = comptador['cini'] or ''
        return cini

    def get_data_comptador(self, polissa_id):
        comp_obj = self.connection.GiscedataLecturesComptador
        comp_id = self.get_comptador(polissa_id)
        data = ''
        if comp_id:
            comp_id = comp_id[0]
            comp = comp_obj.read(comp_id, ['data_alta'])
            data = comp['data_alta'].split('-')
            # Format MM/YYYY
            data = '%s/%s' % (data[1], data[0])
        return data

    def get_cambio_titularidad(self, cups_id):
        O = self.connection
        intervals = O.GiscedataCupsPs.get_modcontractual_intervals
        start = '%s-01-01' % self.year
        end = '%s-12-31' % self.year
        modcons = intervals(cups_id, start, end, {'ffields': ['titular']})
        if len(modcons) > 1:
            return '1'
        else:
            return '0'

    def get_baixa_cups(self, cups_id):
        O = self.connection
        intervals = O.GiscedataCupsPs.get_modcontractual_intervals
        start = '%s-01-01' % self.year
        end = '%s-12-31' % self.year
        modcons = intervals(cups_id, start, end, {'ffields': ['titular']})
        dates = sorted([tuple(x['dates']) for x in modcons.values()])
        days = 1
        for idx, interval in enumerate(dates):
            if idx + 1 == len(dates):
                break
            end = datetime.strptime(interval[-1], '%Y-%m-%d')
            start = datetime.strptime(dates[idx + 1][0], '%Y-%m-%d')
            days = max((start - end).days, days)
        if days > 1:
            return '1'
        else:
            return '0'

    def consumer(self):
        O = self.connection
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                fields_to_read = [
                    'name', 'polissa_polissa', 'cnmc_numero_lectures'
                ]
                cups = O.GiscedataCupsPs.read(item, fields_to_read)

                o_cups = cups['name'][:22]

                if cups['polissa_polissa']:
                    polissa_id = cups['polissa_polissa'][0]
                    o_comptador_cini = self.get_comptador_cini(polissa_id)
                    o_comptador_data = self.get_data_comptador(polissa_id)
                else:
                    o_comptador_cini = ''
                    o_comptador_data = ''
                o_num_lectures = cups['cnmc_numero_lectures'] or ''
                o_titular = self.get_cambio_titularidad(cups['id'])
                o_baixa = self.get_baixa_cups(cups['id'])
                o_year = datetime.now().year

                self.output_q.put([
                    o_cups,
                    o_comptador_cini,
                    o_comptador_data,
                    o_num_lectures,
                    o_titular,
                    o_baixa,
                    o_year
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()