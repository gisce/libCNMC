#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Generació dels CINIS de maquines
"""
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased


class CINIPOS(MultiprocessBased):
    def __init__(self, **kwargs):
        super(CINIPOS, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'CINIPOS'
        self.report_name = 'CNMC INVENTARI CINIPOS'

    def get_sequence(self):
        search_params = [('name', '!=', '1')]
        return self.connection.GiscedataCtsSubestacionsPosicio.search(
            search_params)

    def consumer(self):
        O = self.connection
        fields_to_read = ['cini', 'tensio', 'interruptor', 'tipus_posicio',
                          'barres']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                cini = ''
                trafo = O.GiscedataCtsSubestacionsPosicio.read(
                    item, fields_to_read)

                if not trafo['cini']:
                    #Els cinis de mÃ quines tots comenÃ§en amb I27
                    cini_ = "I28"
                    #Per completar el tercer digit
                    codi3 = ''
                    if 110000 <= trafo['tensio'] < 220000:
                        codi3 = '2'
                    elif 36000 <= trafo['tensio'] < 110000:
                        codi3 = '3'
                    elif 1000 <= trafo['tensio'] < 36000:
                        codi3 = '4'
                    #Per completar el quart digit
                    codi4 = ''
                    if trafo['interruptor']:
                        #els codis  de cini coincideixen amb els que tenim
                        # d'interruptor
                        codi4 = '%s' % trafo['interruptor']
                    #Per completar el cinque digit
                    codi5 = ''
                    if trafo['tipus_posicio'] == 'B':
                        codi5 = '2'
                    elif trafo['tipus_posicio'] == 'C':
                        codi5 = '1'
                    elif trafo['tipus_posicio'] == 'H':
                        codi5 = '3'
                    #Per completar el sise digit
                    codi6 = ''
                    if trafo['barres']:
                        codi6 = '%s' % trafo['barres']

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