#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback, psycopg2.extras
from libcnmc.utils import format_f, convert_srid, get_srid
from libcnmc.core import MultiprocessBased
from shapely import wkt


class FB7(MultiprocessBased):

    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        super(FB7, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB7 - Nudos Topológicos'
        self.base_object = 'Nudos Topológicos'

    def get_sequence(self):

        return self.connection.GiscegisNodes.search([]
        )


    def get_ine_state(self, municipi_id):
        O = self.connection
        data = O.ResMunicipi.read(municipi_id, ['ine', 'dc', 'state'])
        return data

    def get_node_vertex(self, vertex_id):

        v = self.connection.GiscegisVertex.read(vertex_id[0], ['x', 'y'])
        vertex = (round(v['x'], 3), round(v['y'], 3))
        return vertex

    def consumer(self):
        O = self.connection

        fields_to_read = [
            'id', 'name', 'geom', 'vertex', 'tensio', 'municipi_id'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                node = O.GiscegisNodes.read(
                    item, fields_to_read
                )

                #vertex = self.get_node_vertex(node['vertex'])

                coords = wkt.loads(node["geom"]).coords[0]
                vertex = [coords[0], coords[1]]

                if node['municipi_id']:
                   data = self.get_ine_state(node['municipi_id'][0])
                   o_municipi = data['ine']
                   o_provincia = data['state'][0]
                else:
                   o_municipi = ''
                   o_provincia = ''


                z = ''
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(get_srid(O), vertex)

                try:
                    o_tensio = format_f(
                        float(node['tensio']) / 1000.0, decimals=3) or ''
                except:
                    o_tensio = ''

                self.output_q.put([
                    node['name'],                       # NODE
                    format_f(res_srid[0], decimals=3),  # X
                    format_f(res_srid[1], decimals=3),  # Y
                    z,                                  # Z
                    o_tensio,                           # TENSION
                    o_municipi,                         # MUNICIPIO
                    o_provincia,                        # PROVINCIA
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()