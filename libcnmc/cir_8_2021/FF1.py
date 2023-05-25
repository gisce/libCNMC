# -*- coding: utf-8 -*-
import traceback
from datetime import datetime

from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f


class FF1(StopMultiprocessBased):
    def __init__(self, **kwargs):
        super(FF1, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = "Formulario F1: Control de los planes de inversión"
        self.base_object = 'D1'

    def get_sequence(self):
        prevision_ids = self.connection.model('cir8.2021.f1').search(
            [('year', '=', self.year)]
        )
        return prevision_ids

    def consumer(self):
        O = self.connection
        fields_to_read = []
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                d1 = O.model('cir8.2021.f1').read(item, fields_to_read)
                self.output_q.put([
                    format_f(d1['vpi_n_4'], 2),
                    format_f(d1['vpi_n_3'], 2),
                    format_f(d1['vpi_n_2'], 2),
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
