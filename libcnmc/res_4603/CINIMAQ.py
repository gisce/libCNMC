#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Generació dels CINIS de maquines
"""
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased


class CINIMAQ(MultiprocessBased):
    def __init__(self, **kwargs):
        super(CINIMAQ, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'CINIMAQ'
        self.report_name = 'CNMC INVENTARI CINIMAQ'

    def get_sequence(self):
        search_params = [('name', '!=', '1'),
                         ('id_estat.cnmc_inventari', '=', True)]
        return self.connection.GiscedataTransformadorTrafo.search(search_params)

    def consumer(self):
        O = self.connection
        fields_to_read = ['cini', 'tensio_primari_actual', 'tensio_b1',
                          'tensio_b2', 'potencia_nominal', 'reductor']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                cini = ''
                trafo = O.GiscedataTransformadorTrafo.read(
                    item, fields_to_read)

                if not trafo['cini']:
                    #Els cinis de màquines tots començen amb I27
                    cini_ = "I27"
                    #Per completar el tercer digit
                    codi3 = ''
                    if trafo['tensio_primari_actual'] <= 40000:
                        codi3 = '0'
                    elif 220000 <= trafo['tensio_primari_actual'] < 400000:
                        codi3 = '1'
                    elif 110000 <= trafo['tensio_primari_actual'] < 220000:
                        codi3 = '2'
                    elif 36000 <= trafo['tensio_primari_actual'] < 110000:
                        codi3 = '3'
                    elif 1000 <= trafo['tensio_primari_actual'] < 36000:
                        codi3 = '4'
                    #Per completar el quart digit
                    codi4 = ''
                    if trafo['tensio_b1']:
                        if 110000 <= trafo['tensio_b1'] < 220000:
                            codi4 = '2'
                        elif 36000 <= trafo['tensio_b1'] < 110000:
                            codi4 = '3'
                        elif 1000 <= trafo['tensio_b1'] < 36000:
                            codi4 = '4'
                        elif trafo['tensio_b1'] < 1000:
                            codi4 = '4'
                    elif trafo['tensio_b2']:
                        if 110000 <= trafo['tensio_b2'] < 220000:
                            codi4 = '2'
                        elif 36000 <= trafo['tensio_b2'] < 110000:
                            codi4 = '3'
                        elif 1000 <= trafo['tensio_b2'] < 36000:
                            codi4 = '4'
                        elif trafo['tensio_b2'] < 1000:
                            codi4 = '4'
                    #Per completar el cinquè digit
                    codi5 = ''
                    if trafo['reductor']:
                        codi5 = '1'
                    else:
                        codi5 = '2'
                    #Per completar el sisè digit
                    codi6 = ''
                    if trafo['potencia_nominal'] < 1000:
                        codi6 = 'A'
                    elif 1000 <= trafo['potencia_nominal'] < 5000:
                        codi6 = 'B'
                    elif 5000 <= trafo['potencia_nominal'] < 10000:
                        codi6 = 'C'
                    elif 10000 <= trafo['potencia_nominal'] < 15000:
                        codi6 = 'D'
                    elif 15000 <= trafo['potencia_nominal'] < 20000:
                        codi6 = 'E'
                    elif 20000 <= trafo['potencia_nominal'] < 25000:
                        codi6 = 'F'
                    elif 25000 <= trafo['potencia_nominal'] < 30000:
                        codi6 = 'G'
                    elif 30000 <= trafo['potencia_nominal'] < 40000:
                        codi6 = 'H'
                    elif 40000 <= trafo['potencia_nominal'] < 60000:
                        codi6 = 'I'
                    elif 60000 <= trafo['potencia_nominal'] < 80000:
                        codi6 = 'J'
                    elif 80000 <= trafo['potencia_nominal'] < 100000:
                        codi6 = 'K'
                    elif 100000 <= trafo['potencia_nominal'] < 120000:
                        codi6 = 'L'
                    elif 120000 <= trafo['potencia_nominal'] < 150000:
                        codi6 = 'M'
                    elif 150000 >= trafo['potencia_nominal']:
                        codi6 = 'N'

                    cini = cini_ + codi3 + codi4 + codi5 + codi6
                    #El write del cini en cada un dels trafos
                    O.GiscedataTransformadorTrafo.write([item], {'cini': cini})

                self.output_q.put(cini)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()