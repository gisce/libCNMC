# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.utils import format_f, convert_srid, get_srid
from libcnmc.core import MultiprocessBased


class F13(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F13, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F13 - CTS'
        self.base_object = 'CTS'

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

    def get_ines(self, ids):
        o = self.connection
        res = {'ine_municipi': 0, 'ine_provincia': 0}
        if ids.get('id_municipi', False):
            municipi_dict = o.ResMunicipi.read(ids['id_municipi'][0],
                                               ['ine', 'dc'])
            res['ine_municipi'] = '{0}{1}'.format(municipi_dict['ine'][-3:],
                                                municipi_dict['dc'])
        if ids.get('id_provincia', False):
            res['ine_provincia'] = o.ResCountryState.read(
                ids['id_provincia'][0], ['code']
            )['code']
        return res

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
        o_codi_r1 = 'R1-%s' % self.codi_r1[-3:]
        o = self.connection
        fields_to_read = [
            'name', 'cini', 'propietari', 'id_municipi', 'id_provincia',
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
                ids_sub = {
                    'id_municipi': sub['id_municipi'],
                    'id_provincia': sub['id_provincia']
                }
                if "x" in sub and "y" in sub:
                    vertex = (sub["x"], sub["y"])
                else:
                    vertex = self.get_vertex(sub['ct_id'][0])
                ines = self.get_ines(ids_sub)
                o_subestacio = sub['name']
                o_cini = sub['cini']
                o_denominacio = sub['descripcio']
                z = ''
                o_municipi = ines['ine_municipi']
                o_provincia = ines['ine_provincia']
                o_prop = int(sub['propietari'])
                o_any = self.year
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(get_srid(o), vertex)
                self.output_q.put([
                    o_subestacio,                       # SUBESTACION
                    o_cini,                             # CINI
                    o_denominacio,                      # DENOMINACION
                    format_f(res_srid[0], decimals=3),  # X
                    format_f(res_srid[1], decimals=3),  # Y
                    z,                                  # Z
                    o_municipi,                         # MUNICIPIO
                    o_provincia,                        # PROVINCIA
                    o_codi_r1,                          # CODIGO DISTRIBUIDORA
                    o_prop,                             # PROPIEDAD
                    o_any                               # AÑO INFORMACION
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
