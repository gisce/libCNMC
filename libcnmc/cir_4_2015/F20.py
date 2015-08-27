# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import re
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP
from libcnmc.utils import get_ine, get_comptador
from libcnmc.core import MultiprocessBased


class F20(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F20, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'F13 - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        search_params = []
        return self.connection.GiscedataCupsPs.search(search_params)

    def get_cini(self, et):
        o = self.connection
        valor = ''
        if et:
            cts = o.GiscedataCts.search([('name', '=', et)])
            if cts:
                cini = o.GiscedataCts.read(cts[0], ['cini'])
                valor = cini['cini']
        return valor

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'name', 'et'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                cups = o.GiscedataCupsPs.read(
                    item, fields_to_read
                )
                o_codi_r1 = "R1-"+self.codi_r1
                o_cups = cups['name']
                o_cini = self.get_cini(cups['et'])
                if not o_cini:
                    o_cini = 'False'
                o_codi_ct = cups['et']
                self.output_q.put([
                    o_codi_r1,
                    o_cups,
                    o_cini,
                    o_codi_ct
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
