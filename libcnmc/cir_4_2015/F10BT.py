# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.utils import format_f, tallar_text
from libcnmc.core import MultiprocessBased


class F10BT(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F10BT, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F10BT - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        search_params = [
            ('cable.tipus.codi', 'in', ['T', 'D', 'S'])
        ]
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params += ['|', ('data_pm', '=', False),
                               ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                               ('data_baixa', '=', False),
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataBtElement.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_tipus_cable(self, id_cable):
        o = self.connection
        cable = o.GiscedataBtTipuscable.read(id_cable, ['codi'])
        res = ''
        if cable:
            res = cable['codi']
        return res

    def get_provincia(self, id_mun):
        o = self.connection
        res = ''
        prov = o.ResMunicipi.read(id_mun, ['state'])['state'][1]
        if prov:
            res = prov
        return res

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'name', 'propietari', 'coeficient', 'cable', 'voltatge', 'cini',
            'longitud_cad', 'municipi', 'longitud_cad'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                linia = o.GiscedataBtElement.read(item, fields_to_read)

                res = o.GiscegisEdge.search([('id_linktemplate', '=',
                                              linia['name']),
                                             ('layer', 'ilike', '%BT%')])
                if not res:
                    edge = {'start_node': (0, '%s_0' % linia['name']),
                            'end_node': (0, '%s_1' % linia['name'])}
                elif len(res) > 1:
                    edge = {'start_node': (0, '%s_0' % linia['name']),
                            'end_node': (0, '%s_1' % linia['name'])}
                else:
                    edge = o.GiscegisEdge.read(res[0], ['start_node',
                                                        'end_node'])

                o_prop = linia['propietari'] and '1' or '0'
                # Coeficient per ajustar longituds de trams
                coeficient = linia['coeficient'] or 1.0
                # Comprovar el tipus del cable
                fields_to_read_cable = [
                    'tipus', 'reactancia', 'resistencia', 'intensitat_admisible'
                ]
                # Agafem el cable de la linia
                cable = o.GiscedataBtCables.read(linia['cable'][0],
                                                 fields_to_read_cable)
                #Agafem la tensió
                try:
                    o_nivell_tensio = format_f(
                        (int(linia['voltatge']) / 1000.0), 3
                    )
                except:
                    o_nivell_tensio = 0.0
                o_tram = 'B%s' % linia['name']
                o_node_inicial = tallar_text(edge['start_node'][1], 20)
                o_node_final = tallar_text(edge['end_node'][1], 20)
                o_cini = linia['cini']
                o_provincia = self.get_provincia(linia['municipi'][0])
                o_longitud = format_f(
                    round(linia['longitud_cad'] * coeficient / 1000.0, 3), 3
                ) or 0.001
                o_num_circuits = 1  # a BT suposarem que sempre hi ha 1
                o_tipus = self.get_tipus_cable(cable['tipus'][0])
                o_r = format_f(
                    cable['resistencia'] * linia['longitud_cad'], 6) or 0.0
                o_x = format_f(
                    cable['reactancia'] * linia['longitud_cad'], 6) or 0.0
                o_int_max = format_f(cable['intensitat_admisible'], 3)
                o_op_habitual = 1  # Tots son actius
                o_cod_dis = 'R1-%s' % self.codi_r1[-3:]
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
