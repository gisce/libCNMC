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
    """
    Class that generates the Despachos(6) file of the 4131
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(DES, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies DES'
        self.report_name = 'CNMC INVENTARI DES'

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        """
        search_params = [('any_ps', '<=', self.year)]
        return self.connection.GiscedataDespatx.search(search_params)

    def consumer(self):
        """
        Method that generates the csb file
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'denominacio', 'any_ps', 'vai', 'data_apm'
        ]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                despatx = O.GiscedataDespatx.read(
                    item, fields_to_read)
                tmp_date = datetime.strptime(despatx['data_apm'], '%Y-%m-%d')
                data_apm = tmp_date.strftime('%d/%m/%Y')
                if despatx['data_apm'] < '{0}-01-01'.format(self.year):
                    estado = '0'
                else:
                    estado = '2'
                fecha_baja = ''
                output = [
                    '{0}'.format(despatx['name']),
                    despatx['cini'] or '',
                    despatx['denominacio'] or '',
                    data_apm,
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
