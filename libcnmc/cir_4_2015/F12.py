# -*- coding: utf-8 -*-
from datetime import datetime
import traceback

from libcnmc.utils import format_f
from libcnmc.core import MultiprocessBased


class F12(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F12, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F12 - CTS'
        self.base_object = 'CTS'

    def get_sequence(self):
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params = [
            ('reductor', '=', False),
            ('id_estat.cnmc_inventari', '=', True)
        ]
        search_params += ['|', ('data_pm', '=', False),
                          ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                          ('data_baixa', '=', False)
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataTransformadorTrafo.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_node(self, trafo_id):
        """
        Returns the node of the CT

        :param trafo_id: Id of the trafo
        :type trafo_id: int

        :return: Node of the CT
        :rtype: int
        """

        o = self.connection
        trafo = o.GiscedataTransformadorTrafo.read(trafo_id, ["ct"])
        if trafo.get("ct"):
            ct_id = trafo.get("ct")[0]
            ct = o.GiscedataCts.read(ct_id, ["node_id"])
            return ct["node_id"][1]

        bloc = o.GiscegisBlocsTransformadors.search(
            [('transformadors', '=', trafo_id)])
        node = ''
        if bloc:
            bloc_vals = o.GiscegisBlocsTransformadors.read(
                bloc[0], ['node'])
            node = bloc_vals['node'][1]
        return node

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'ct', 'name', 'cini', 'potencia_nominal', 'propietari',
            'perdues_buit', 'perdues_curtcircuit_nominal',
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                trafo = o.GiscedataTransformadorTrafo.read(
                    item, fields_to_read
                )
                o_ct = trafo['ct'] and trafo['ct'][1] or ''

                o_node = self.get_node(item)
                o_node = o_node.replace('*', '')
                o_cini = trafo['cini'] or ''
                o_maquina = trafo['name']
                o_pot = format_f(
                    float(trafo['potencia_nominal']),
                    decimals=3
                )
                o_perdues_buit = format_f(
                    trafo['perdues_buit'] or 0.0,
                    decimals=3
                )
                o_perdues_nominal = format_f(
                    trafo['perdues_curtcircuit_nominal'] or 0.0,
                    decimals=3
                )
                o_propietari = int(trafo['propietari'])
                o_any = self.year

                self.output_q.put([
                    o_node,             # NUDO
                    o_ct,               # CT
                    o_cini,             # CINI
                    o_maquina,          # MAQUINA
                    o_pot,              # POT MAQUINA
                    o_perdues_buit,     # PERDIDAS VACIO
                    o_perdues_nominal,  # PERDIDAS POT NOMINAL
                    o_propietari,       # PROPIEDAD
                    o_any               # AÑO INFORMACION
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
