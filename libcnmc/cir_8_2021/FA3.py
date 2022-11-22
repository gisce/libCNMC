# -*- coding: utf-8 -*-
from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f, convert_srid, get_srid
from datetime import datetime
from shapely import wkt
import traceback


class FA3(MultiprocessBased):

    def __init__(self, **kwargs):
        super(FA3, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'Formulario A3: Previsiones de crecimiento de mercado'
        self.base_object = 'Previsiones de crecimiento'

    def get_sequence(self):
        prevision_ids = self.connection.GiscedataPrevisioCreixement.search([('year_previsto', '>', self.year)])
        return prevision_ids

    def consumer(self):
        O = self.connection
        fields_to_read = ['name', 'tipo_crecimiento', 'node_id', 'superficie', 'uso_previsto', 'potencia_solicitada',
                          'id_municipi', 'id_provincia', 'year_previsto', 'suministros_bt', 'suministros_mt',
                          'suministros_at']
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                prevision = O.GiscedataPrevisioCreixement.read(item, fields_to_read)

                # CÓDIGO CRECIMIENTO
                o_cod_crecimiento = ''
                if prevision['name']:
                    o_cod_crecimiento = prevision['name']

                # TIPO CRECIMIENTO
                o_tipo_crecimiento = ''
                if prevision['tipo_crecimiento']:
                    o_tipo_crecimiento = prevision['tipo_crecimiento']

                # COORDENADAS
                res_srid = ['', '']
                if prevision["node_id"]:
                    node_data = O.GiscegisNodes.read(prevision["node_id"][0], ["geom"])
                    coords = wkt.loads(node_data["geom"]).coords[0]
                    vertex = [coords[0], coords[1]]
                    if vertex:
                        res_srid = convert_srid(get_srid(O), vertex)

                # SUPERFÍCIE
                o_superficie = ''
                if prevision['superficie']:
                    o_superficie = prevision['superficie']

                # USO
                o_uso = ''
                if prevision['uso_previsto']:
                    o_uso = prevision['uso_previsto']

                # POTENCIA SOLICITADA
                o_potencia_solicitada = ''
                if prevision['potencia_solicitada']:
                    o_potencia_solicitada = prevision['potencia_solicitada']

                # MUNICIPIO
                o_municipio = ''
                if prevision['id_municipi']:
                    o_municipio = prevision['id_municipi']

                # PROVINCIA
                o_provincia = ''
                if prevision['id_provincia']:
                    o_provincia = prevision['id_provincia']

                # AÑO PREVISTO
                o_year_previsto = ''
                if prevision['year_previsto']:
                    o_year_previsto = prevision['year_previsto']

                # SUMINISTROS BT
                o_suministros_bt = ''
                if prevision['suministros_bt']:
                    o_suministros_bt = prevision['suministros_bt']

                # SUMINISTROS MT
                o_suministros_mt = ''
                if prevision['suministros_mt']:
                    o_suministros_mt = prevision['suministros_mt']

                # SUMINISTROS AT
                o_suministros_at = ''
                if prevision['suministros_at']:
                    o_suministros_at = prevision['suministros_at']

                self.output_q.put([
                    o_cod_crecimiento,                  # CÓDIGO CRECIMIENTO
                    o_tipo_crecimiento,                 # TIPO CRECIMIENTO
                    format_f(res_srid[0], decimals=3),  # COORDENADA X
                    format_f(res_srid[1], decimals=3),  # COORDENADA Y
                    '',                                 # COORDENADA Z
                    o_superficie,                       # SUPERFICIE
                    o_uso,                              # USO
                    o_potencia_solicitada,              # POTENCIA SOLICITADA
                    o_municipio,                        # MUNICIPIO
                    o_provincia,                        # PROVINCIA
                    o_year_previsto,                    # AÑO PREVISTO
                    o_suministros_bt,                   # SUMINISTROS BT
                    o_suministros_mt,                   # SUMINISTROS MT
                    o_suministros_at,                   # SUMINISTROS AT
                ])

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
