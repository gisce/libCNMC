#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Despatxos
"""
from __future__ import absolute_import
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f, get_forced_elements
from libcnmc.models import F6Res4666


class DES(MultiprocessBased):
    """
    Class that generates the Despachos(6) file of the 4666
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
        self.compare_field = kwargs["compare_field"]

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        """
        data_limit = '01-01-{}'.format(self.year+1)
        search_params = [('data_apm', '<=', data_limit)]

        ids = self.connection.GiscedataDespatx.search(search_params)

        forced_ids = get_forced_elements(self.connection, "giscedata.despatx")

        ids = ids + forced_ids["include"]
        ids = list(set(ids) - set(forced_ids["exclude"]))

        return list(set(ids))

    def consumer(self):
        """
        Method that generates the csb file
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'denominacio', 'any_ps', 'vai', 'data_apm',
            self.compare_field
        ]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                despatx = O.GiscedataDespatx.read(
                    item, fields_to_read)
                tmp_date = datetime.strptime(despatx['data_apm'], '%Y-%m-%d')
                data_apm = tmp_date.strftime('%d/%m/%Y')
                fecha_baja = ''

                if despatx[self.compare_field]:
                    data_entregada = despatx[self.compare_field]
                    entregada = F6Res4666(**data_entregada)
                    actual = F6Res4666(
                        despatx['name'],
                        despatx['cini'],
                        despatx['denominacio'],
                        data_apm,
                        fecha_baja,
                        format_f(despatx['vai']),
                        0
                    )
                    if actual == entregada:
                        estado = '0'
                    else:
                        estado = '1'
                else:
                    if despatx['data_apm']:
                        if despatx['data_apm'][:4] != str(self.year):
                            estado = '1'
                        else:
                            estado = '2'
                    else:
                        estado = '1'
                output = [
                    '{0}'.format(despatx['name']),  # IDENTIFICADOR
                    despatx['cini'] or '',          # CINI
                    despatx['denominacio'] or '',   # DENOMINACION
                    data_apm,                       # FECHA APS
                    fecha_baja,                     # FECHA BAJA
                    format_f(despatx['vai']),    # VALOR INVERSION
                    estado                          # ESTADO
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
