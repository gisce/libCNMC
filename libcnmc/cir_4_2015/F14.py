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
            'conexions', 'energia_anual', 'potencia_activa',
            'potencia_reactiva', 'perdues_buit', 'perdues_curtcircuit_nominal'
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
                o_pot_maquina = format_f(
                    float(trafo['potencia_nominal']) / 1000.0, decimals=3)
                o_pot_activa = format_f(
                    float(trafo['potencia_activa']), decimals=3)
                o_pot_reactiva = format_f(
                    float(trafo['potencia_reactiva']), decimals=3)
                o_energia_anual = format_f(
                    float(trafo['energia_anual']), decimals=3)
                o_perdues = format_f(
                    float(trafo['perdues_buit']), decimals=3)
                o_perdues_nominal = format_f(
                    float(
                        trafo['perdues_curtcircuit_nominal']
                    ), decimals=3)
                o_propietat = int(trafo['propietari'])
                o_estat = self.get_estat(trafo['id_estat'][0])
                o_any = self.year

                self.output_q.put([
                    o_subestacio,           # SUBESTACION
                    o_maquina,              # MAQUINA
                    o_cini,                 # CINI
                    o_costat_alta,          # PARQUE ALTA
                    o_costat_baixa,         # PARQUE BAJA
                    o_pot_maquina,          # POTENCIA MAQUINA
                    o_pot_activa,           # POTENCIA ACTIVA
                    o_pot_reactiva,         # POTENCIA REACTIVA
                    o_energia_anual,        # ENERGIA ANUAL CIRCULADA
                    o_perdues,              # PERDIDAS DE VACIO
                    o_perdues_nominal,      # PERDIDAS POTENCIA NOMINAL
                    o_propietat,            # PROPIEDAD
                    o_estat,                # ESTADO
                    o_any                   # AÑO INFORMACION
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
