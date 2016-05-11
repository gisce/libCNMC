#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Despatxos
"""
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f


class DES(MultiprocessBased):
    def __init__(self, **kwargs):
        super(DES, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies DES'
        self.report_name = 'CNMC INVENTARI DES'

    def get_sequence(self):
        search_params = [('any_ps', '<=', self.year)]
        return self.connection.GiscedataDespatx.search(search_params)

    def consumer(self):
        O = self.connection
        fields_to_read = ['name', 'cini', 'denominacio', 'any_ps', 'vai']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                despatx = O.GiscedataDespatx.read(
                    item, fields_to_read)
                if despatx['any_ps'] == self.year:
                    estado = '2'
                else:
                    estado = '0'
                fecha_baja = ''
                output = [
                    '{0}'.format(despatx['name']),
                    despatx['cini'] or '',
                    despatx['denominacio'] or '',
                    despatx['any_ps'],
                    fecha_baja,
                    format_f(despatx['vai']),
                    estado
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
