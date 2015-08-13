# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import re
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP
from libcnmc.utils import get_ine, get_comptador
from libcnmc.core import MultiprocessBased


class F13c(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F13c, self).__init__(**kwargs)

    def get_sequence(self):
        search_params = []
        return self.connection.GiscedataCupsPs.search(search_params)

    def consumer(self):
        o = self.connection
        while True:
            try:
                # generar linies
                o_subestacio = ''
                o_parc = ''
                o_pos = ''
                o_cini = ''
                o_prop = ''
                o_data = ''
                o_any = ''

                self.output_q.put([
                    o_subestacio,
                    o_parc,
                    o_pos,
                    o_cini,
                    o_prop,
                    o_data,
                    o_any
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
