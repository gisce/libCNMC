# -*- coding: utf-8 -*-
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f, convert_srid, get_srid, get_ine
from datetime import datetime
from shapely import wkt
import traceback


class FA3(StopMultiprocessBased):

    def __init__(self, **kwargs):
        super(FA3, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'Formulario A3: Previsiones de crecimiento de mercado'
        self.base_object = 'Previsiones de crecimiento'

    def get_sequence(self):
        prevision_ids = self.connection.GiscedataPrevisioCreixement.search([('formulario_year', '=', self.year)])
        return prevision_ids

    def get_ine(self, municipi_id):
        """
        Returns the INE code of the given municipi
        :param municipi_id: Id of the municipi
        :type municipi_id: int
        :return: state, ine municipi
        :rtype:tuple
        """
        O = self.connection
        muni = O.ResMunicipi.read(municipi_id, ['ine'])
        return get_ine(O, muni['ine'])

    def consumer(self):
        O = self.connection
        fields_to_read = ['name', 'tipo_crecimiento', 'node_id', 'superficie', 'uso_previsto', 'potencia_solicitada',
                          'id_municipi', 'id_provincia', 'year_previsto', 'suministros_bt', 'suministros_mt',
                          'suministros_at', 'x', 'y']
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
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
                o_x = ''
                if prevision.get('x', False):
                    o_x = prevision['x']

                o_y = ''
                if prevision.get('y', False):
                    o_y = prevision['y']

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

                # MUNICIPIO / PROVINCIA
                o_codi_ine_mun = ''
                o_codi_ine_prov = ''
                if prevision['id_municipi']:
                    id_municipio = prevision['id_municipi'][0]
                    o_codi_ine_prov, o_codi_ine_mun = self.get_ine(id_municipio)

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
                    o_cod_crecimiento,                              # CÓDIGO CRECIMIENTO
                    o_tipo_crecimiento,                             # TIPO CRECIMIENTO
                    format_f(o_x, decimals=3),                      # COORDENADA X
                    format_f(o_y, decimals=3),                      # COORDENADA Y
                    '0,000',                                             # COORDENADA Z
                    format_f(o_superficie, decimals=3),             # SUPERFICIE
                    o_uso,                                          # USO
                    format_f(o_potencia_solicitada, decimals=3),    # POTENCIA SOLICITADA
                    o_codi_ine_mun,                                 # MUNICIPIO
                    o_codi_ine_prov,                                # PROVINCIA
                    o_year_previsto,                                # AÑO PREVISTO
                    o_suministros_bt,                               # SUMINISTROS BT
                    o_suministros_mt,                               # SUMINISTROS MT
                    o_suministros_at,                               # SUMINISTROS AT
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
