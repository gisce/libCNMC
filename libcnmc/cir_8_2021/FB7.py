#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback, psycopg2.extras
from libcnmc.utils import format_f, convert_srid, get_srid, get_ine
from libcnmc.core import StopMultiprocessBased
from shapely import wkt


class FB7(StopMultiprocessBased):

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
        fields_to_read = [
            'id', 'name', 'geom', 'vertex', 'tensio', 'municipi_id'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                node = O.GiscegisNodes.read(
                    item, fields_to_read
                )

                coords = wkt.loads(node["geom"]).coords[0]
                vertex = [coords[0], coords[1]]

                # MUNICIPI I PROVINCIA
                o_municipi = ''
                o_provincia = ''
                if node.get('municipi_id', False):
                    municipi_id = node['municipi_id'][0]
                    o_provincia, o_municipi = self.get_ine(municipi_id)

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
                    '0,000',                            # Z
                    o_tensio,                           # TENSION
                    o_municipi,                         # MUNICIPIO
                    o_provincia,                        # PROVINCIA
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
