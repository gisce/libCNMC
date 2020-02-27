#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC AT
"""
import traceback
from os import environ

from libcnmc.utils import format_f, get_name_ti, get_codigo_ccaa
from libcnmc.core import MultiprocessBased


class CTS(MultiprocessBased):
    """
    Class to generate installation report for CTS (8)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs:
        :type kwargs: dict
        """

        self.year = kwargs.pop("year")
        self.price_accuracy = environ.get('OPENERP_OBRES_PRICE_ACCURACY', 3)
        super(CTS, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        installations_ids = self.connection.GiscedataProjecteObra.get_audit_installations_by_year(
            [], self.year, [8]
        )

        return installations_ids[8]

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
            'tipo_inversion',
            'ccuu',
            'codigo_ccaa',
            'nivel_tension_explotacion',
            'financiado',
            'fecha_aps',
            'fecha_baja',
            'causa_baja',
            'im_ingenieria',
            'im_materiales',
            'im_obracivil',
            'im_trabajos',
            'subvenciones_europeas',
            'subvenciones_nacionales',
            'valor_auditado',
            'valor_contabilidad',
            'cuenta_contable',
            'porcentaje_modificacion',
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataProjecteObraTiCts.read([item], fields_to_read)[0]
                output = [
                    linia['name'],
                    linia['cini'],
                    linia['tipo_inversion'],
                    get_name_ti(O, linia['ccuu'] and linia['ccuu'][0]),
                    linia['codigo_ccaa'],
                    linia['nivel_tension_explotacion'],
                    linia['financiado'],
                    linia['fecha_aps'],
                    linia['fecha_baja'],
                    linia['causa_baja'],
                    format_f(linia['im_ingenieria'] or 0.0,
                             self.price_accuracy),
                    format_f(linia['im_materiales'] or 0.0,
                             self.price_accuracy),
                    format_f(linia['im_obracivil'] or 0.0, self.price_accuracy),
                    format_f(linia['im_trabajos'] or 0.0, self.price_accuracy),
                    format_f(linia['subvenciones_europeas'] or 0.0, self.price_accuracy),
                    format_f(linia['subvenciones_nacionales'] or 0.0, self.price_accuracy),
                    format_f(linia['valor_auditado'] or 0.0,
                             self.price_accuracy),
                    format_f(linia['valor_contabilidad'] or 0.0,
                             self.price_accuracy),
                    linia['cuenta_contable'],
                    linia['porcentaje_modificacion'],
                ]
                output = map(lambda e: e or '', output)
                self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
