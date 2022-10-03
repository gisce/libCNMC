#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback, psycopg2.extras
from libcnmc.utils import format_f, convert_srid, get_srid, get_ines
from libcnmc.core import MultiprocessBased

ZONA = {
    'RURAL CONCENTRADA': 'RC',
    'RURAL DISPERSA': 'RD',
    'URBANA': 'U',
    'SEMIURBANA': 'SU'
}

class FB3(MultiprocessBased):

    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        super(FB3, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB3 - SE'
        self.base_object = 'SE'

    def get_sequence(self):
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params = [('ct_id.propietari', '=', True),
                         '|', ('ct_id.data_pm', '=', False),
                         ('ct_id.data_pm', '<', data_pm),
                         '|', ('ct_id.data_baixa', '>', data_baixa),
                         ('ct_id.data_baixa', '=', False),
                         ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('ct_id.active', '=', False),
                          ('ct_id.data_baixa', '!=', False),
                          ('ct_id.active', '=', True)]
        return self.connection.GiscedataCtsSubestacions.search(
            search_params, 0, 0, False, {'active_test': False})

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

    def get_ct(self, ct_id):
        o = self.connection
        ct = o.GiscedataCts.search([('id', '=', ct_id)])
        return ct

    def consumer(self):
        o_codi_r1 = 'R1-%s' % self.codi_r1[-3:]
        o = self.connection

        fields_to_read = [
            'name', 'cini', 'propietari', 'id_municipi', 'id_provincia', 'punt_frontera',
            'ct_id', 'descripcio', "x", "y"
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                sub = o.GiscedataCtsSubestacions.read(
                    item, fields_to_read
                )

                o_subestacio = sub['name']
                o_cini = sub['cini']
                o_denominacio = sub['descripcio']
                o_prop = int(sub['propietari'])

                ids_sub = {
                    'id_municipi': sub['id_municipi'],
                    'id_provincia': sub['id_provincia']
                }
                if "x" in sub and "y" in sub:
                    vertex = (sub["x"], sub["y"])
                else:
                    vertex = self.get_vertex(sub['ct_id'][0])
                ines = get_ines(o, ids_sub)
                o_municipi = ines['ine_municipi']
                o_provincia = ines['ine_provincia']

                z = ''
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(get_srid(o), vertex)

                ct = self.get_ct(sub['ct_id'][0])[0]
                data_ct = o.GiscedataCts.read(ct, ['zona_id', 'punt_frontera'])
                o_punt_frontera = int(data_ct['punt_frontera'] == True)
                zona = data_ct['zona_id'][1]
                if zona:
                    o_zona = ZONA[zona]
                else:
                    o_zona = ""

                self.output_q.put([
                    o_subestacio,                       # SUBESTACION
                    o_cini,                             # CINI
                    o_denominacio,                      # DENOMINACION
                    o_punt_frontera,                    # PUNTO FRONTERA
                    format_f(res_srid[0], decimals=3),  # X
                    format_f(res_srid[1], decimals=3),  # Y
                    z,                                  # Z
                    o_municipi,                         # MUNICIPIO
                    o_provincia,                        # PROVINCIA
                    o_zona,                             # ZONA
                    o_prop,                             # PROPIEDAD
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()