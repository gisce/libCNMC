# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import re
import traceback

from libcnmc.utils import CODIS_TARIFA, CODIS_ZONA, CINI_TG_REGEXP
from libcnmc.utils import get_ine, get_comptador
from libcnmc.core import MultiprocessBased


class F15(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F15, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'F15 - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        search_params = [
            # ('inventari', '=', 'fiabilitat'),
            ('installacio', 'ilike', 'giscedata.at.suport,%')
        ]
        return self.connection.GiscedataCellesCella.search(search_params)

    def get_node_vertex(self, installacio):
        o = self.connection
        valor = str.split(installacio, ',')
        id_tram = int(valor[1])
        node = ''
        vertex = ('', '')
        if id_tram:
            bloc = o.GiscegisBlocsCtat.read(id_tram, ['node', 'vertex'])
            if bloc:
                node = bloc['node'][0]
                if bloc['vertex']:
                    v = o.GiscegisVertex.read(bloc['vertex'][0], ['x', 'y'])
                    vertex = (round(v['x'], 3), round(v['y'], 3))
        return node, vertex

    def obtenir_tram(self, installacio):
        o = self.connection
        valor = str.split(installacio, ',')
        id_tram = int(valor[1])
        tram = o.GiscedataAtSuport.read(id_tram, ['name'])
        valor = tram['name']
        return valor

    def obtenir_camps_linia(self, installacio):
        o = self.connection
        valor = str.split(installacio, ',')
        id_tram = int(valor[1])
        tram = o.GiscedataAtSuport.read(id_tram, ['linia'])
        linia_id = tram['linia']
        fields_to_read = [
            'municipi', 'provincia', 'tensio'
        ]
        linia = o.GiscedataAtLinia.read(int(linia_id[0]), fields_to_read)
        municipi = linia['municipi'][0]
        provincia = linia['provincia'][0]
        tensio = linia['tensio']
        res = {
            'municipi': municipi,
            'provincia': provincia,
            'tensio': tensio
        }
        return res

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'inventari', 'installacio', 'cini', 'propietari'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                celles = o.GiscedataCellesCella.read(
                    item, fields_to_read
                )
                dict_linia = self.obtenir_camps_linia(celles['installacio'])
                o_node, vertex = self.get_node_vertex(celles['installacio'])
                o_fiabilitat = celles['inventari']
                o_tram = self.obtenir_tram(celles['installacio'])
                o_cini = celles['cini']
                o_utm_x = vertex[0]
                o_utm_y = vertex[1]
                o_utm_z = ''
                o_municipi = dict_linia.get('municipi')
                o_provincia = dict_linia.get('provincia')
                o_tensio = dict_linia.get('tensio')
                o_cod_dis = self.codi_r1
                o_prop = int(celles['propietari'])
                o_any = self.year + 1

                self.output_q.put([
                    o_node,
                    o_fiabilitat,
                    o_tram,
                    o_cini,
                    o_utm_x,
                    o_utm_y,
                    o_utm_z,
                    o_municipi,
                    o_provincia,
                    o_tensio,
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
