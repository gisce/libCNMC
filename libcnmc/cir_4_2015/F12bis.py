# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import re
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP
from libcnmc.utils import get_ine, get_comptador
from libcnmc.core import MultiprocessBased


class F12bis(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F12bis, self).__init__(**kwargs)

    def get_sequence(self):
        search_params = []
        return self.connection.GiscedataCupsPs.search(search_params)

    def consumer(self):
        o = self.connection
        while True:
            try:
                # generar linies
                o_ct = ''
                o_maquina = ''
                o_posicio = ''
                o_cini = ''
                o_propietat = ''
                o_data = ''
                o_any = ''

                self.output_q.put([
                    o_ct,
                    o_maquina,
                    o_posicio,
                    o_cini,
                    o_propietat,
                    o_data,
                    o_any
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
