# -*- coding: utf-8 -*-
from libcnmc.core import MultiprocessBased
from libcnmc.utils import tallar_text, format_f
from datetime import datetime
import traceback

ZONA = {
    'RURAL CONCENTRADA': 'RC',
    'RURAL DISPERSA': 'RD',
    'URBANA': 'U',
    'SEMIURBANA': 'SU'
}


class FA5(MultiprocessBased):

    def __init__(self, **kwargs):
        super(FA5, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'Formulario A5: Información relativa a la energia intercambiada en los puntos frontera'
        self.base_object = 'Punt frontera'

    def get_sequence(self):
        O = self.connection
        ids_tipus = O.GiscedataPuntFronteraTipus.search([('retribucions', '=', True)])
        ids = O.GiscedataPuntFrontera.search([('tipus', 'in', ids_tipus), ('any_publicacio', '=', self.year)])
        return ids

    def consumer(self):
        O = self.connection
        fields_to_read = ['element', 'name', 'zona', 'tipus_frontera', 'tensio_id', 'codigo_empresa',
                          'codigo_frontera_dt']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                punt_frontera = O.GiscedataPuntFrontera.read()

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
                        o_identificador = identificador['name']

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
                    o_tipo_frontera = punt_frontera['tipus_frontera'].upper

                # TENSIÓN
                o_tension = ''
                if punt_frontera.get('tensio_id', False):
                    o_tension = format_f(float(punt_frontera['tensio_id'][1]) / 1000, 3)

                # ENERGIA ACTIVA ENTRANTE
                o_energia_activa_entrante = ''
                o_energia_activa_saliente = ''
                o_energia_reactiva_entrante = ''
                o_energia_reactiva_saliente = ''

                # CÓDIGO EMPRESA
                o_codigo_empresa = ''
                if punt_frontera.get('codigo_empresa', False):
                    o_codigo_empresa = punt_frontera['codigo_empresa']

                # CÓDIGO FRONTERA DT
                o_codigo_frontera_dt = ''
                if punt_frontera.get('codigo_frontera_dt', False):
                    o_codigo_frontera_dt = punt_frontera['codigo_frontera_dt']

                self.output_q.put([
                    tallar_text(o_identificador, 22),   # IDENTIFICADOR
                    o_denominacion,                     # DENOMINACIÓN
                    o_zona,                             # ZONA
                    o_tipo_frontera,                    # TIPO FRONTERA
                    o_tension,                          # TENSIÓN
                    o_energia_activa_entrante,          # ENERGIA ACTIVA ENTRANTE
                    o_energia_activa_saliente,          # ENERGIA ACTIVA SALIENTE
                    o_energia_reactiva_entrante,        # ENERGIA REACTIVA ENTRANTE
                    o_energia_reactiva_saliente,        # ENERGIA REACTIVA SALIENTE
                    o_codigo_empresa,                   # CÓDIGO EMPRESA
                    o_codigo_frontera_dt,               # CÓDIGO EMPRESA
                ])

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
