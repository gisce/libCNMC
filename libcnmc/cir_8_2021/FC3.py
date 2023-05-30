# -*- coding: utf-8 -*-
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f
from datetime import datetime
import traceback


class FC3(StopMultiprocessBased):

    def __init__(self, **kwargs):
        super(FC3, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = ('Formulario C3: Detalle de inversión, '
                            'gastos e ingresos en atención de la prestación de '
                            'servicios por la distribuidora que tienen '
                            'establecido un precio regulado. '
                            'Derechos de extensión')
        self.base_object = 'Operaciones partes vinculadas'

    def get_sequence(self):
        c3_ids = self.connection.model('cir8.2021.c3').search(
            [('year', '=', self.year)]
        )
        return c3_ids

    def consumer(self):
        O = self.connection
        fields_to_read = [
            'cia', 'actuaciones', 'tension', 'potencia',
            'inversion_n_5', 'inversion_n_4', 'inversion_n_3',
            'inversion_n_2', 'gasto', 'ingreso', 'identificador'
        ]
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                c3 = O.model('cir8.2021.c3').read(item, fields_to_read)
                self.output_q.put([
                    c3.get('cia', ''),
                    c3.get('actuaciones', 0),
                    format_f(c3.get('tension', 0.0), 3),
                    format_f(c3.get('potencia', 0.0), 3),
                    format_f(c3.get('inversion_n_5', 0.0), 2),
                    format_f(c3.get('inversion_n_4', 0.0), 2),
                    format_f(c3.get('inversion_n_3', 0.0), 2),
                    format_f(c3.get('inversion_n_2', 0.0), 2),
                    format_f(c3.get('gastos', 0.0), 2),
                    format_f(c3.get('ingreso', 0.0), 2),
                    c3.get('identificador', ''),
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
