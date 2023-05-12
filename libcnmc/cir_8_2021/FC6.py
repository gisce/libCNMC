# -*- coding: utf-8 -*-
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f
from datetime import datetime
import traceback


class FC6(StopMultiprocessBased):

    def __init__(self, **kwargs):
        super(FC6, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = (
            "Formulario C6: Información relativa a activos enajenados"
        )
        self.base_object = 'Formulario C6'

    def get_sequence(self):
        c6_ids = self.connection.model('cir8.2021.c6').search(
            [('year', '=', self.year)]
        )
        return c6_ids

    def consumer(self):
        O = self.connection
        fields_to_read = [
            'cod_operacion', 'tipo_operacion', 'coco', 'descripcion',
            'fecha_inicio', 'fecha_fin', 'ingreso_contrato', 'ingreso',
            'inmovilizado', 'gasto'
        ]
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                c6 = O.model('cir8.2021.c6').read(item, fields_to_read)

                # CÓDIGO OPERACION
                o_cod_operacion = ''
                if c6.get('cod_operacion', False):
                    o_cod_operacion = c6['cod_operacion']

                # TIPO OPERACION
                o_tipo_operacion = ''
                if c6.get('o_tipo_operacion', False):
                    o_tipo_operacion = c6['tipo_operacion']

                # COCO
                o_coco = ''
                if c6.get('coco', False):
                    o_coco = c6['coco']

                # DESCRIPCION
                o_descripcion = ''
                if c6.get('descripcion', False):
                    o_descripcion = c6['descripcion']

                # FECHA INICIO
                o_fecha_inicio = '00/00/0000'
                if c6.get('fecha_inicio', False):
                    fecha_inicio_p = datetime.strptime(
                        str(c6['fecha_inicio']),
                        '%Y-%m-%d')
                    o_fecha_inicio = fecha_inicio_p.strftime('%d/%m/%Y')

                # FECHA FIN
                o_fecha_fin = ''
                if c6.get('fecha_fin', False):
                    fecha_fin_p = datetime.strptime(
                        str(c6['fecha_fin']),
                        '%Y-%m-%d')
                    o_fecha_fin = fecha_fin_p.strftime('%d/%m/%Y')

                # INGRESO CONTRATO
                o_ingreso_contrato = ''
                if c6.get('ingreso_contrato', False):
                    o_ingreso_contrato = format_f(
                        c6['ingreso_contrato'], decimals=2
                    )

                # INGRESO
                o_ingreso = ''
                if c6.get('ingreso', False):
                    o_ingreso = format_f(c6['ingreso'], decimals=2)

                # INMOVILIZADO
                o_inmovilizado = ''
                if c6.get('inmovilizado', False):
                    o_inmovilizado = format_f(c6['inmovilizado'], decimals=2)

                # GASTO
                o_gasto = ''
                if c6.get('gasto', False):
                    o_gasto = format_f(c6['gasto'], decimals=2)

                self.output_q.put([
                    o_cod_operacion,            # CÓDIGO OPERACION
                    o_tipo_operacion,           # TIPO OPERACION
                    o_coco,                     # COCO
                    o_descripcion,              # DESCRIPCION
                    o_fecha_inicio,             # FECHA INICIO
                    o_fecha_fin,                # FECHA FIN
                    o_ingreso_contrato,         # INGRESO CONTRATO
                    o_ingreso,                  # INGRESO
                    o_inmovilizado,             # INMOVILIZADO
                    o_gasto,                    # GASTO
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
