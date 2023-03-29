# -*- coding: utf-8 -*-
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import tallar_text, format_f
from datetime import datetime
import traceback

ZONA = {
    'RURAL CONCENTRADA': 'RC',
    'RURAL DISPERSA': 'RD',
    'URBANA': 'U',
    'SEMIURBANA': 'SU'
}


class FA5(StopMultiprocessBased):

    def __init__(self, **kwargs):
        super(FA5, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'Formulario A5: Información relativa a la energia intercambiada en los puntos frontera'
        self.base_object = 'Punt frontera'

    def get_sequence(self):
        O = self.connection
        ids_tipus = O.GiscedataPuntFronteraTipus.search([('retribucio', '=', True)])
        ids = O.GiscedataPuntFrontera.search([('tipus', 'in', ids_tipus)])
        return ids

    def consumer(self):
        O = self.connection
        fields_to_read = ['element', 'name', 'zona', 'tipus_frontera', 'tensio_id', 'codigo_empresa',
                          'codigo_frontera_dt']
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                punt_frontera = O.GiscedataPuntFrontera.read(item, fields_to_read)

                # IDENTIFICADOR
                o_identificador = ''
                if punt_frontera.get('element', False):
                    element = punt_frontera['element']
                    element = element.split(',')
                    model = element[0]
                    obj_id = int(element[1])
                    model_obj = O.model(model)

                    identificador = model_obj.read(obj_id, ['name'])
                    if identificador.get('name', False):
                        o_identificador = str(identificador['name'])

                # DENOMINACIÓN
                o_denominacion = ''
                if punt_frontera.get('name', False):
                    o_denominacion = punt_frontera['name']

                # ZONA
                o_zona = ''
                if punt_frontera.get('zona', False):
                    o_zona = punt_frontera['zona'].upper()

                # TIPO FRONTERA
                o_tipo_frontera = ''
                if punt_frontera.get('tipus_frontera', False):
                    o_tipo_frontera = punt_frontera['tipus_frontera'].upper()

                # TENSIÓN
                o_tension = ''
                if punt_frontera.get('tensio_id', False):
                    o_tension = format_f(float(punt_frontera['tensio_id'][1]) / 1000, 3)

                # ENERGIA ACTIVA ENTRANTE
                fields_to_read_energia = [
                    'data_inicial', 'data_final', 'activa_entrant', 'activa_sortint', 'reactiva_entrant',
                    'reactiva_sortint'
                ]
                energia_obj = O.GiscedataPuntFronteraMesures
                energia_ids = energia_obj.search([('punt_frontera_id', '=', item)])
                energia_data = energia_obj.read(energia_ids, fields_to_read_energia)

                o_energia_activa_entrante = 0
                o_energia_activa_saliente = 0
                o_energia_reactiva_entrante = 0
                o_energia_reactiva_saliente = 0
                inici_any = '{}-01-01'.format(self.year)
                fi_any = '{}-12-31'.format(self.year)

                for energia in energia_data:
                    if inici_any <= energia['data_inicial'] <= fi_any and inici_any <= energia['data_final'] <= fi_any:
                        if energia.get('activa_entrant', False):
                            o_energia_activa_entrante += energia['activa_entrant']
                        if energia.get('activa_sortint', False):
                            o_energia_activa_saliente += energia['activa_sortint']
                        if energia.get('reactiva_entrant', False):
                            o_energia_reactiva_entrante += energia['reactiva_entrant']
                        if energia.get('reactiva_sortint', False):
                            o_energia_reactiva_saliente += energia['reactiva_sortint']

                # CÓDIGO EMPRESA
                o_codigo_empresa = ''
                if punt_frontera.get('codigo_empresa', False):
                    participant_obj = O.GiscemiscParticipant
                    participant_id = punt_frontera['codigo_empresa'][0]
                    participant_data = participant_obj.read(participant_id, ['ref2'])
                    if participant_data.get('ref2', False):
                        o_codigo_empresa = participant_data['ref2']

                # CÓDIGO FRONTERA DT
                o_codigo_frontera_dt = ''
                if punt_frontera.get('codigo_frontera_dt', False):
                    o_codigo_frontera_dt = punt_frontera['codigo_frontera_dt']

                self.output_q.put([
                    tallar_text(o_identificador, 22),                   # IDENTIFICADOR
                    o_denominacion,                                     # DENOMINACIÓN
                    o_zona,                                             # ZONA
                    o_tipo_frontera,                                    # TIPO FRONTERA
                    o_tension,                                          # TENSIÓN
                    format_f(o_energia_activa_entrante, decimals=3),    # ENERGIA ACTIVA ENTRANTE
                    format_f(o_energia_activa_saliente, decimals=3),    # ENERGIA ACTIVA SALIENTE
                    format_f(o_energia_reactiva_entrante, decimals=3),  # ENERGIA REACTIVA ENTRANTE
                    format_f(o_energia_reactiva_saliente, decimals=3),  # ENERGIA REACTIVA SALIENTE
                    o_codigo_empresa,                                   # CÓDIGO EMPRESA
                    o_codigo_frontera_dt,                               # CÓDIGO EMPRESA
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
