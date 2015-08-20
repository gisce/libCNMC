# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.utils import format_f, get_norm_tension
from libcnmc.core import MultiprocessBased


class F14(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F14, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F14 - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        search_params = [
            ('reductor', '=', True),
            ('id_estat.cnmc_inventari', '=', True)
        ]
        return self.connection.GiscedataTransformadorTrafo.search(
            search_params
        )

    def get_estat(self, estat_id):
        o = self.connection
        estat = o.GiscedataTransformadorEstat.read(estat_id, ['codi'])
        if estat['codi'] != 1:
            return 0
        else:
            return 1

    def get_costat_alta(self, trafo):
        o = self.connection
        res = ''
        if trafo['conexions']:
            con = o.GiscedataTransformadorConnexio.read(trafo['conexions'][0])
            tensio = con['tensio_primari']
            res = get_norm_tension(o, tensio)
        return res

    def get_costat_baixa(self, trafo):
        o = self.connection
        res = ''
        if trafo['conexions']:
            con = o.GiscedataTransformadorConnexio.read(trafo['conexions'][0])
            tensio = con['tensio_b1']
            res = get_norm_tension(o, tensio)
        return res


    def consumer(self):
        o = self.connection
        fields_to_read = [
            'ct', 'name', 'cini', 'potencia_nominal', 'propietari', 'id_estat',
            'conexions'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                trafo = o.GiscedataTransformadorTrafo.read(
                    item, fields_to_read
                )
                o_subestacio = trafo['ct'][1]
                o_maquina = trafo['name']
                o_cini = trafo['cini']
                o_costat_alta = self.get_costat_alta(trafo)
                o_costat_baixa = self.get_costat_baixa(trafo)
                o_pot_maquina = format_f(trafo['potencia_nominal'] / 1000.0, 3)
                o_pot_activa = ''
                o_pot_reactiva = ''
                o_energia_anual = ''
                o_perdues = ''
                o_perdues_nominal = ''
                o_propietat = int(trafo['propietari'])
                o_estat = self.get_estat(trafo['id_estat'][0])
                o_any = self.year + 1

                self.output_q.put([
                    o_subestacio,
                    o_maquina,
                    o_cini,
                    o_costat_alta,
                    o_costat_baixa,
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
