# -*- coding: utf-8 -*-
import traceback
from datetime import datetime

from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f


class FC1(StopMultiprocessBased):
    def __init__(self, **kwargs):
        super(FC1, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = (
            "Formulario C1: Gastos e ingresos operativos de la actividad de"
            " distribución"
        )
        self.base_object = 'C1'

    def get_sequence(self):
        prevision_ids = self.connection.model('cir8.2021.c1').search(
            [('year', '=', self.year)]
        )
        return prevision_ids

    def get_ine_ccaa(self, ccaa_id):
        O = self.connection
        codi_ccaa = O.model('res.comunitat_autonoma').read(ccaa_id, ['codi'])
        return codi_ccaa['codi']

    def consumer(self):
        O = self.connection
        fields_to_read = []
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                c1 = O.model('cir8.2021.c1').read(item, fields_to_read)

                # CUENTA
                o_cuenta = ''
                if c1.get('cuenta', False):
                    o_cuenta = c1['cuenta']

                # CUENTA PGC
                o_cuenta_pgc = ''
                if c1.get('cuenta_pgc', False):
                    o_cuenta_pgc = c1['cuenta_pgc']

                # DESCRIPCION
                o_descripcion = ''
                if c1.get('descripcion', False):
                    o_descripcion = c1['descripcion']

                # CECO
                o_ceco = ''
                if c1.get('ceco', False):
                    o_ceco = c1['ceco']

                # COCO
                o_coco = ''
                if c1.get('coco', False):
                    o_coco = c1['coco']

                # PRESTACION SERVICIO
                o_prestacion_servicio = ''
                if c1.get('prestacion_servicio', False):
                    o_prestacion_servicio = c1['prestacion_servicio']

                # GASTO
                o_gasto = ''
                if c1.get('gasto', False):
                    o_gasto = format_f(c1['gasto'], decimals=2)

                # INGRESO
                o_ingreso = ''
                if c1.get('ingreso', False):
                    o_ingreso = format_f(c1['ingreso'], decimals=2)


                # CINI
                o_cini = ''
                if c1.get('cini', False):
                    o_cini = c1['cini']

                # TIPO COSTE
                o_tipo_coste = ''
                if c1.get('tipo_coste', False):
                    o_tipo_coste = c1['tipo_coste']

                # CCAA
                o_ccaa = ''
                if c1.get('ccaa', False):
                    o_ccaa = self.get_ine_ccaa(c1['ccaa'][0])

                # UNIDADES
                o_unidades = ''
                if c1.get('unidades', False):
                    o_unidades = c1['unidades']

                self.output_q.put([
                    o_cuenta,           # CUENTA
                    o_cuenta_pgc,           # CUENTA PGC
                    o_descripcion,          # DESCRIPCION
                    o_ceco,                 # CECO
                    o_coco,                 # COCO
                    o_prestacion_servicio,  # PRESTACION SERVICIO
                    o_gasto,                # GASTO
                    o_ingreso,              # INGRESO
                    o_cini,                 # CINI
                    o_tipo_coste,           # TIPO COSTE
                    o_ccaa,                 # CCAA
                    o_unidades,             # UNIDADES
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
