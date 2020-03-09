#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback
from os import environ

from libcnmc.utils import format_f, get_name_ti, get_codigo_ccaa
from libcnmc.core import MultiprocessBased


class SE(MultiprocessBased):
    """
    Class to generate installation report for SE (3)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        self.year = kwargs.pop("year")
        self.price_accuracy = int(environ.get('OPENERP_OBRES_PRICE_ACCURACY', '3'))
        super(SE, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        installations_ids = self.connection.GiscedataProjecteObra.get_audit_installations_by_year(
            [], self.year, [3]
        )

        return installations_ids[3]

    def consumer(self):
        """
        Generates the line of the file
        :return: Line
        :rtype: str
        """

        O = self.connection
        fields_to_read = [
            'name',
            'cini',
            'denominacion',
            'tipo_suelo',
            'planificacion',
            'identificador_parque_1',
            'identificador_parque_2',
            'identificador_parque_3',
            'identificador_parque_4',
            'cini_parque_1',
            'cini_parque_2',
            'cini_parque_3',
            'cini_parque_4',
            'num_posiciones_parque_1',
            'num_posiciones_parque_2',
            'num_posiciones_parque_3',
            'num_posiciones_parque_4',
            'pn_transformacion',
            'pn_reactancias',
            'pn_condensadores',
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataProjecteObraTiSubestacions.read(
                    item, fields_to_read
                )
                output = [
                    linia['name'],
                    linia['cini'],
                    linia['denominacion'],
                    linia['tipo_suelo'],
                    linia['planificacion'],
                    linia['identificador_parque_1'],
                    linia['identificador_parque_2'],
                    linia['identificador_parque_3'],
                    linia['identificador_parque_4'],
                    linia['cini_parque_1'],
                    linia['cini_parque_2'],
                    linia['cini_parque_3'],
                    linia['cini_parque_4'],
                    linia['num_posiciones_parque_1'],
                    linia['num_posiciones_parque_2'],
                    linia['num_posiciones_parque_3'],
                    linia['num_posiciones_parque_4'],
                    linia['pn_transformacion'],
                    linia['pn_reactancias'],
                    linia['pn_condensadores'],
                ]
                output = map(lambda e: e or '', output)
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
