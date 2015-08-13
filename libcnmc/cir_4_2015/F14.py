# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import re
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP
from libcnmc.utils import get_ine, get_comptador
from libcnmc.core import MultiprocessBased


class F14(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F14, self).__init__(**kwargs)

    def get_sequence(self):
        search_params = []
        return self.connection.GiscedataCupsPs.search(search_params)

    def consumer(self):
        o = self.connection
        while True:
            try:
                # generar linies
                o_subestacio = ''
                o_maquina = ''
                o_alta = ''
                o_baixa = ''
                o_pot_maquina = ''
                o_pot_activa = ''
                o_pot_reactiva = ''
                o_energia_anual = ''
                o_perdues = ''
                o_perdues_nominal = ''
                o_propietat = ''
                o_estat = ''
                o_any = ''

                self.output_q.put([
                    o_subestacio,
                    o_maquina,
                    o_alta,
                    o_baixa,
                    o_pot_maquina,
                    o_pot_activa,
                    o_pot_reactiva,
                    o_energia_anual,
                    o_perdues,
                    o_perdues_nominal,
                    o_propietat,
                    o_estat,
                    o_any
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
