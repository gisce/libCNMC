# -*- coding: utf-8 -*-
import traceback
from datetime import datetime

from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f


class FC7(StopMultiprocessBased):
    def __init__(self, **kwargs):
        super(FC7, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = """Formulario C7: Detalle de los inmovilizados 
        brutos declarados para la actividad de distribución"""
        self.base_object = 'C7'

    def get_sequence(self):
        prevision_ids = self.connection.model('cir8.2021.c7').search(
            [('formulario_year', '=', self.year)]
        )
        return prevision_ids

    def consumer(self):
        O = self.connection
        fields_to_read = [
            'cuenta',
            'cuenta_pgc',
            'descripcion',
            'tipo_inmovilizado',
            'grado_amortizacion',
            'financiacion',
            'inversion_year',
            'amortizacion_year',
            'traspaso_year',
            'baja_year',
            'inmovilizado',
            'amortizacion_acumulada'
        ]
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                c7 = O.model('cir8.2021.c7').read(item, fields_to_read)

                # cuenta
                o_cuenta = ''
                if c7['o_cuenta']:
                    o_cuenta = c7['o_cuenta']

                # cuenta_pgc
                o_cuenta_pgc = ''
                if c7['cuenta_pgc']:
                    o_cuenta_pgc = c7['cuenta_pgc']

                # descripcion
                o_descripcion = ''
                if c7['descripcion']:
                    o_descripcion = c7['descripcion']

                # tipo_inmovilizado
                o_tipo_inmovilizado = ''
                if c7['tipo_inmovilizado']:
                    o_tipo_inmovilizado = c7['tipo_inmovilizado']

                # grado_amortizacion
                o_grado_amortizacion = ''
                if c7['grado_amortizacion']:
                    o_grado_amortizacion = c7['grado_amortizacion']

                # financiacion
                o_financiacion = ''
                if c7['financiacion']:
                    o_financiacion = c7['financiacion']

                # inversion_year
                o_inversion_year = ''
                if c7['inversion_year']:
                    o_inversion_year = c7['inversion_year']

                # amortizacion_year
                o_amortizacion_year = ''
                if c7['amortizacion_year']:
                    o_amortizacion_year = c7['amortizacion_year']

                # traspaso_year
                o_traspaso_year = ''
                if c7['traspaso_year']:
                    o_traspaso_year = c7['traspaso_year']

                # baja_year
                o_baja_year = ''
                if c7['baja_year']:
                    o_baja_year = c7['baja_year']

                # inmovilizado
                o_inmovilizado = ''
                if c7['inmovilizado']:
                    o_inmovilizado = c7['inmovilizado']

                # amortizacion_acumulada
                o_amortizacion_acumulada = ''
                if c7['amortizacion_acumulada']:
                    o_amortizacion_acumulada = c7['amortizacion_acumulada']

                self.output_q.put([
                    o_cuenta,
                    o_cuenta_pgc,
                    o_descripcion,
                    o_tipo_inmovilizado,
                    o_grado_amortizacion,
                    format_f(o_financiacion, decimals=2),
                    format_f(o_inversion_year, decimals=2),
                    format_f(o_amortizacion_year, decimals=2),
                    format_f(o_traspaso_year, decimals=2),
                    format_f(o_baja_year, decimals=2),
                    format_f(o_inmovilizado, decimals=2),
                    format_f(o_amortizacion_acumulada, decimals=2)
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
