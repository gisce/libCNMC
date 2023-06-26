#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Informe de la CNMC relatiu a la generació conectada de les xarxes de distribució
"""
from __future__ import absolute_import
from datetime import datetime, timedelta
import traceback
from libcnmc.core import StopMultiprocessBased
from workalendar.europe import Spain

ACCEPTED_STATES = [
    'done',
    'pending'
]


class FD2(StopMultiprocessBased):

    def __init__(self, **kwargs):
        super(FD2, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1') or ''
        self.year = kwargs.pop('year', datetime.now().year - 2)
        self.report_name = 'FD2 - Calidad Comercial'
        self.base_object = 'Códigos de gestión de Calidad'

    def get_sequence(self):

        """
            Retorna la llista de ID's per al metode consumer.
            :return: List of ID's
            :rtype: list(int)
        """

        search_params = [
            '|',
            ('active', '=', True),
            ('year', '=', self.year),
            ('cod_gestion.name', '!=', 'Z8_01_dl15')
        ]
        codis_gestio = self.connection.model('cir8.2021.d2').search(search_params)

        return codis_gestio

    def consumer(self):

        o = self.connection

        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                o = self.connection

                d2_obj = o.model('cir8.2021.d2')

                cod_gest_data = d2_obj.read(item, [])
                cod_gest_name = o.model('giscedata.codigos.gestion.calidad.z').read(cod_gest_data['cod_gestion'][0], ['name'])['name']

                ## Asignem els valors segons escau
                if cod_gest_name != 'Z8_01_dl5':
                    output = [
                        cod_gest_name,
                        cod_gest_data['solicitudes'],
                        cod_gest_data['en_plazo'],
                        cod_gest_data['fuera_plazo'],
                        cod_gest_data['no_atendidas'],
                    ]
                    self.output_q.put(output)
                else:
                    d2_z8_15 = d2_obj.search([('active', '=', True), ('cod_gestion.name', '=', 'Z8_01_dl15')])[0]
                    d2_z8_15 = d2_obj.read(d2_z8_15, [])
                    solicitudes = cod_gest_data['solicitudes'] + d2_z8_15['solicitudes']
                    en_plazo = cod_gest_data['en_plazo'] + d2_z8_15['en_plazo']
                    fuera_plazo = cod_gest_data['fuera_plazo'] + d2_z8_15['fuera_plazo']
                    no_atendidas = cod_gest_data['no_atendidas'] + d2_z8_15['no_atendidas']

                    output = [
                        'Z8_01',
                        solicitudes,
                        en_plazo,
                        fuera_plazo,
                        no_atendidas

                    ]
                    self.output_q.put(output)

                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
