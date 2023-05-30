# -*- coding: utf-8 -*-
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f
from datetime import datetime
import traceback


class FC4(StopMultiprocessBased):

    def __init__(self, **kwargs):
        super(FC4, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = ('Formulario C4: Detalle de inversión, '
                            'gastos e ingresos en atención de la prestación de '
                            'servicios por la distribuidora que tienen '
                            'establecido un precio regulado. '
                            'Derechos de extensión')
        self.base_object = 'Operaciones partes vinculadas'

    def get_sequence(self):
        c4_ids = self.connection.model('cir8.2021.c4').search(
            [('year', '=', self.year)]
        )
        return c4_ids

    def consumer(self):
        O = self.connection
        fields_to_read = [
            'cia', 'actuaciones', 'inversion',
            'gasto', 'ingreso', 'gasto', 'tension', 'potencia'
        ]
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                c4 = O.model('cir8.2021.c4').read(item, fields_to_read)
                row = []
                for field in fields_to_read:
                    if field in ['tension', 'potencia']:
                        row.append(
                            format_f(c4.get(field, 0.0), 3)
                        )
                    elif field in ['cia', 'actuaciones']:
                        row.append(c4.get(field, ''))
                    else:
                        row.append(
                            format_f(c4.get(field, 0.0), 2)
                        )
                self.output_q.put(row)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
