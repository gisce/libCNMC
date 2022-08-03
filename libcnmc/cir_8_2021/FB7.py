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

    def get_voltatge_node(self, node_id):

        edge = self.connection.GiscegisEdge.search([('start_node', '=', node_id)])[0]
        if not edge:
            edge = self.connection.GiscegisEdge.search([('end_node', '=', node_id)])[0]
        if edge:
            tram = self.connection.GiscedataAtTram.search([('edge_id', '=', edge)])
            if not tram:
                tram = self.connection.GiscedataBtElement.search([('edge_id', '=', edge)])
            if tram:
                print(tram)
                print(tram[0])
                res = self.connection.GiscedataTensionsTensio.read(tram[0], ['tensio_id'])['tensio_id']
                print(res)
                return res

    def get_node_vertex(self, vertex_id):

        v = self.connection.GiscegisVertex.read(vertex_id[0], ['x', 'y'])
        vertex = (round(v['x'], 3), round(v['y'], 3))
        return vertex

    def consumer(self):
        O = self.connection

        fields_to_read = [
            'id', 'name', 'geom', 'vertex'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                node = O.GiscegisNodes.read(
                    item, fields_to_read
                )

                vertex = self.get_node_vertex(node['vertex'])

                voltatge = self.get_voltatge_node(node['id'])

                #ines = self.get_ines(ids_sub)

                z = ''
                #o_municipi = ines['ine_municipi']
                #o_provincia = ines['ine_provincia']
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(get_srid(O), vertex)



                self.output_q.put([
                    node['name'],                       # SUBESTACION
                    format_f(res_srid[0], decimals=3),  # X
                    format_f(res_srid[1], decimals=3),  # Y
                    z,                                  # Z
                    #TENSION
                    # o_municipi,                         # MUNICIPIO
                    #o_provincia,                        # PROVINCIA
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()