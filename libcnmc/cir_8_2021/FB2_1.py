#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback

from libcnmc.utils import format_f
from libcnmc.core import StopMultiprocessBased

OPERACIO = {
    'Reserva': '0',
    'Operativo': '1',
}

class FB2_1(StopMultiprocessBased):
    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(FB2_1, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'CTS'
        self.report_name = 'Formulario B2.1: Máquinas en Centros de Transformación'
        self.compare_field = ""
        self.extended = kwargs.get("extended", False)

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        :rtype: list[int]
        """

        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params = [
            ('reductor', '=', False),
            ('id_estat.cnmc_inventari', '=', True),
            ('propietari', '=', True),
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

    def get_operacio(self, id_estat):

        o = self.connection
        operacio = o.GiscedataTransformadorEstat.read(
            id_estat[0], ['operacio']
        )['operacio']
        return operacio


    def consumer(self):
        """
        Method that generates the csv file
        :return: List of arrays
        """
        o = self.connection
        fields_to_read = [
            'ct', 'name', 'cini', 'potencia_nominal',
            'data_pm', 'id_estat', 'id_regulatori'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                trafo = o.GiscedataTransformadorTrafo.read(
                    item, fields_to_read
                )

                o_ct = trafo['ct'] and trafo['ct'][1] or ''
                o_cini = trafo['cini'] or ''
                o_maquina = trafo['id_regulatori'] if trafo.get('id_regulatori', False) else trafo['name']
                o_pot = format_f(
                    float(trafo['potencia_nominal']),
                    decimals=3
                )

                tmp_date = datetime.strptime(
                    trafo['data_pm'], '%Y-%m-%d')
                o_any = tmp_date.strftime('%Y')

                id_estat = trafo['id_estat']

                if id_estat:
                    o_operacio = self.get_operacio(id_estat)
                else:
                    o_operacio = 0

                if o_cini:
                    if o_cini[1] == '2' and o_cini[2] == '7' and o_cini[5] == '2' and o_cini[7] == '1':
                        o_operacio = 0

                self.output_q.put([
                    o_ct,           # CT
                    o_maquina,      # MAQUINA
                    o_cini,         # CINI
                    o_pot,          # POT MAQUINA
                    o_any,          # AÑO INFORMACION
                    o_operacio     # OPERACION
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
