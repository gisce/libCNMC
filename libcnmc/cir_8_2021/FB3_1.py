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

class FB3_1(MultiprocessBased):

    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        super(FB3_1, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB3_1 - PARCS'
        self.base_object = 'PARCS'

    def get_sequence(self):
        # Revisem que estigui actiu
        search_params = [('active', '!=', False)]
        return self.connection.GiscedataParcs.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_subestacio(self, sub_id):
        """
        Returns the SE data
        :param sub_id: ID of SE
        :type sub_id: int
        :return: Node, Name, CINI and CT-ID of the SE
        :rtype: dict[str,str]
        """

        o = self.connection
        sub = o.GiscedataCtsSubestacions.read(
            sub_id, ['ct_id', 'cini', 'name', 'node_id', 'x', 'y']
        )
        ret = {
            "ct_id": sub['ct_id'][0],
            "cini": sub['cini'],
            "name": sub['name'],
            "x": sub['x'],
            "y": sub['y'],
        }
        if 'node_id' in sub:
            ret["node"] = sub["node_id"][1]
        else:
            bloc_ids = o.GiscegisBlocsCtat.search([('ct', '=', ret["ct_id"])])
            node = ''
            if bloc_ids:
                bloc = o.GiscegisBlocsCtat.read(bloc_ids[0], ['node'])
                node = bloc['node'][1]
            ret["node"] = node
        return ret

    def get_tensio(self, parc_id):
        o = self.connection
        tensio_id = o.GiscedataParcs.read(
            parc_id, ['tensio_id'])['tensio_id'][0]
        return o.GiscedataTensionsTensio.read(tensio_id, ['tensio'])['tensio']

    def get_vertex(self, ct_id):
        o = self.connection
        bloc = o.GiscegisBlocsCtat.search([('ct', '=', ct_id)])
        vertex = ('', '')
        if bloc:
            bloc = o.GiscegisBlocsCtat.read(bloc[0], ['vertex'])
            if bloc['vertex']:
                v = o.GiscegisVertex.read(bloc['vertex'][0], ['x', 'y'])
                vertex = (round(v['x'], 3), round(v['y'], 3))
        return vertex


    def consumer(self):
        o = self.connection
        fields_to_read = [
            'id', 'subestacio_id', 'name', 'propietari', 'cini', 'tensio_const'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                parc = o.GiscedataParcs.read(
                    item, fields_to_read
                )

                subestacio = self.get_subestacio(parc['subestacio_id'][0])
                o_subestacio = subestacio['name']

                if "x" in subestacio and "y" in subestacio:
                    vertex = (subestacio["x"], subestacio["y"])
                else:
                    vertex = self.get_vertex(subestacio['ct_id'])
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(get_srid(o), vertex)
                z = ''

                o_parc = parc['name']
                o_node = subestacio['node']
                o_node = o_node.replace('*', '')
                o_cini = parc['cini']
                tensio = self.get_tensio(parc['id'])
                o_tensio = format_f(
                    float(tensio) / 1000.0, decimals=3)
                o_prop = int(parc['propietari'])
                o_tensio_const = parc['tensio_const']

                if o_tensio_const:
                    if tensio != o_tensio_const:
                        o_tensio_const = format_f(
                            float(o_tensio_const) / 1000.0, decimals=3)
                    else:
                        o_tensio_const = ''
                else:
                    o_tensio_const = ''


                self.output_q.put([
                    o_subestacio,  # SUBESTACION
                    o_parc,  # PARQUE
                    o_node,  # NUDO
                    format_f(res_srid[0], decimals=3),  # X
                    format_f(res_srid[1], decimals=3),  # Y
                    z,  # Z
                    o_cini,  # CINI
                    o_tensio,  # TENSION DEL PARQUE
                    o_tensio_const, #TENSION DE CONSTRUCCION DEL PARQUE
                    o_prop,  # PROPIEDAD
                ])
            except Exception as e:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
