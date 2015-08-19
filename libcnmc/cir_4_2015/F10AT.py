# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.utils import format_f
from libcnmc.core import MultiprocessBased


class F10AT(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F10AT, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F10 - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        search_params = [('name', '!=', '1'), ('propietari', '=', True)]
        return self.connection.GiscedataAtLinia.search(search_params)

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'name', 'cini', 'circuits', 'longitud_cad', 'linia', 'origen',
            'final', 'coeficient', 'cable', 'tensio_max_disseny'
        ]
        data_pm_limit = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        static_search_params = [('propietari', '=', True),
                                '|', ('data_pm', '=', False),
                                     ('data_pm', '<', data_pm_limit),
                                '|', ('data_baixa', '>', data_baixa),
                                     ('data_baixa', '=', False),
                                ]
        # Revisem que si està de baixa ha de tenir la data informada.
        static_search_params += ['|',
                                 '&', ('active', '=', False),
                                      ('data_baixa', '!=', False),
                                 ('active', '=', True)]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                linia = o.GiscedataAtLinia.read(
                    item, ['trams', 'tensio', 'municipi', 'propietari',
                           'provincia']
                )

                o_prop = linia['propietari'] and '1' or '0'
                search_params = [('id', 'in', linia['trams'])]
                search_params += static_search_params
                ids = o.GiscedataAtTram.search(
                    search_params, 0, 0, False, {'active_test': False})
                for at in o.GiscedataAtTram.read(ids, fields_to_read):
                    # Coeficient per ajustar longituds de trams
                    coeficient = at['coeficient'] or 1.0
                    # Comprovar el tipus del cable
                    fields_to_read_cable = [
                        'tipus', 'resistencia', 'reactancia',
                        'intensitat_admisible'
                    ]
                    cable = o.GiscedataAtCables.read(at['cable'][0],
                                                     fields_to_read_cable)
                    o_tipus = o.GiscedataAtTipuscable.read(cable['tipus'][0],
                                                         ['codi'])['codi']
                    # Si el tram tram es embarrat no l'afegim
                    if o_tipus == 'E':
                        continue
                    #Agafem la tensió
                    o_nivell_tensio = (
                        (at['tensio_max_disseny'] or linia['tensio']) / 1000.0)
                    o_nivell_tensio = format_f(o_nivell_tensio)
                    o_tram = 'A%s' % at['name']
                    o_node_inicial = at['origen'][0:20]
                    o_node_final = at['final'][0:20]
                    o_cini = at['cini']
                    o_provincia = linia['provincia'][1]
                    o_longitud = round(
                        at['longitud_cad'] * coeficient / 1000.0, 3
                    ) or 0.001
                    o_num_circuits = at['circuits']
                    o_r = cable['resistencia'] or ''
                    o_x = cable['reactancia'] or ''
                    o_int_max = format_f(cable['intensitat_admisible'])
                    o_op_habitual = 1  # Tots son actius
                    o_cod_dis = self.codi_r1
                    o_any = self.year + 1

                    self.output_q.put([
                        o_tram,
                        o_node_inicial,
                        o_node_final,
                        o_cini,
                        o_provincia,
                        o_nivell_tensio,
                        o_longitud,
                        o_num_circuits,
                        o_tipus,
                        o_r,
                        o_x,
                        o_int_max,
                        o_op_habitual,
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
