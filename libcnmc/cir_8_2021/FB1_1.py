# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
from libcnmc.utils import convert_srid, get_srid, format_f

class FB1_1(MultiprocessBased):
    def __init__(self, **kwargs):
        super(FB1_1, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB1.1 - Topología real'
        self.base_object = 'Topología'
        self.prefix = kwargs.pop('prefix', 'A') or 'A'

    def get_sequence(self):
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params = ['|', ('data_pm', '=', False),
                         ('data_pm', '<', data_pm),
                         '|', ('data_baixa', '>', data_baixa),
                         ('data_baixa', '=', False),
                         ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                          ('data_baixa', '!=', False),
                          ('active', '=', True)]
        res_at = self.connection.GiscedataAtTram.search(
            search_params, 0, 0, False, {'active_test': False})
        res_bt = self.connection.GiscedataBtElement.search(
            search_params, 0, 0, False, {'active_test': False})
        at = [(r, 'at') for r in res_at]
        bt = [(r, 'bt') for r in res_bt]
        return at + bt

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'id', 'name', 'geom'
        ]
        while True:
            try:
                # generar tramos
                item = self.input_q.get()
                self.progress_q.put(item)
                if item[1] == 'at':
                    tramo = o.GiscedataAtTram.read(item[0], fields_to_read)
                elif item[1] == 'bt':
                    tramo = o.GiscedataBtElement.read(item[0], fields_to_read)
                o_segmento = tramo['name']
                o_identificador = '{}{}'.format(self.prefix, tramo['name']) 
                geom = tramo['geom']
                points = geom.replace('LINESTRING(', '')
                points = points.replace(')', '')
                points = points.split(',')
                o_position = 1
                o_nsegmento = len(points) - 1
                next_ = None

                for o_position, p in enumerate(points):
                    try:
                        dest = points[o_position + 1]
                        if dest:
                            # Vertex inici
                            o_inicio = '{} 0'.format(p)
                            inicio = o_inicio.split(' ')
                            vertex_inicio = {
                                'x': inicio[0],
                                'y': inicio[1]
                            }
                            res_srid_inicio = convert_srid(get_srid(o), (vertex_inicio['x'], vertex_inicio['y']))
                            o_inicio_z = ''

                            # Vertex final
                            o_final = '{} 0'.format(dest)
                            final = o_final.split(' ')
                            vertex_final = {
                                'x': final[0],
                                'y': final[1]
                            }
                            res_srid_final = convert_srid(get_srid(o), (vertex_final['x'], vertex_final['y']))
                            o_final_z = ''

                            self.output_q.put([
                                '{}_{}'.format(o_segmento, o_position + 1),  # CÓDIGO SEGMENTO
                                o_identificador,                             # IDENTIFICADOR DE TRAMO
                                o_position + 1,                              # ORDEN EN LA LISTA DE SEGMENTOS
                                o_nsegmento,                                 # TOTAL SEGMENTOS
                                format_f(res_srid_inicio[0], decimals=3),    # COORDENADA X INICIO
                                format_f(res_srid_inicio[1], decimals=3),    # COORDENADA Y INICIO
                                o_inicio_z,                                  # COORDENADA Z INICIO
                                format_f(res_srid_final[0], decimals=3),     # COORDENADA X FINAL
                                format_f(res_srid_final[1], decimals=3),     # COORDENADA Y FINAL
                                o_final_z                                    # COORDENADA Z FINAL
                            ])
                    except:
                        pass


            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
