# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import re
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP
from libcnmc.utils import get_ine, get_comptador
from libcnmc.core import MultiprocessBased


class F10(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F10, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F10 - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        search_params = []
        return self.connection.GiscedataCupsPs.search(search_params)

    def consumer(self):
        o = self.connection
        while True:
            try:
                # generar linies
                o_tram = ''
                o_node_inicial = ''
                o_node_final = ''
                o_cini = ''
                o_provincia = ''
                o_nivell_tensio = ''
                o_longitud = ''
                o_num_circuits = ''
                o_tipus = ''
                o_r = ''
                o_x = ''
                o_int_max = ''
                o_op_habitual = ''
                o_cod_dis = self.codi_r1
                o_prop = ''
                o_any = self.year + 1

                self.output_q.put([
                    o_tram,
                    o_node_inicial,
                    o_node_final,
                    o_cini,
                    o_provincia,
                    o_nivell_tensio,
                    o_longitud,
                    o_num_circuits,
                    o_tipus,
                    o_r,
                    o_x,
                    o_int_max,
                    o_op_habitual,
                    o_cod_dis,
                    o_prop,
                    o_any
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
