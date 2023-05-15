# -*- coding: utf-8 -*-
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f, convert_srid, get_srid, get_ine
from datetime import datetime
from shapely import wkt
import traceback


class FC5(StopMultiprocessBased):

    def __init__(self, **kwargs):
        super(FC5, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'Formulario C5: Información relativa a operaciones con partes vinculadas'
        self.base_object = 'Operaciones partes vinculadas'

    def get_sequence(self):
        c5_ids = self.connection.model('cir8.2021.c5').search([('year', '=', self.year)])
        return c5_ids

    def consumer(self):
        O = self.connection
        fields_to_read = ['cod_operacion', 'parte_vinculada', 'cuenta', 'cuenta_pgc', 'ceco', 'coco', 'razon_social',
                          'cif_nif', 'descripcion', 'margen', 'gasto', 'ingreso']
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                c5 = O.model('cir8.2021.c5').read(item, fields_to_read)

                # COD_OPERACION
                o_cod_operacion = ''
                if c5.get('cod_operacion', False):
                    o_cod_operacion = c5['cod_operacion']

                # PARTE_VINCULADA
                o_parte_vinculada = ''
                if c5.get('parte_vinculada', False):
                    o_parte_vinculada = c5['parte_vinculada']

                # CUENTA
                o_cuenta = ''
                if c5.get('cuenta', False):
                    o_cuenta = c5['cuenta']

                # CUENTA_PGC
                o_cuenta_pgc = ''
                if c5.get('cuenta_pgc', False):
                    o_cuenta_pgc = c5['cuenta_pgc']

                # CECO
                o_ceco = ''
                if c5.get('ceco', False):
                    o_ceco = c5['ceco']

                # COCO
                o_coco = ''
                if c5.get('coco', False):
                    o_coco = c5['coco']

                # RAZON_SOCIAL
                o_razon_social = ''
                if c5.get('razon_social', False):
                    o_razon_social = c5['razon_social']

                # CIF_NIF
                o_cif_nif = ''
                if c5.get('cif_nif', False):
                    o_cif_nif = c5['cif_nif']

                # DESCRIPCION
                o_descripcion = ''
                if c5.get('descripcion', False):
                    o_descripcion = c5['descripcion']

                # MARGEN
                o_margen = ''
                if c5.get('margen', False):
                    o_margen = c5['margen']

                # GASTO
                o_gasto = ''
                if c5.get('gasto', False):
                    o_gasto = c5['gasto']

                # INGRESO
                o_ingreso = ''
                if c5.get('ingreso', False):
                    o_ingreso = c5['ingreso']

                self.output_q.put([
                    o_cod_operacion,                    # COD_OPERACION
                    o_parte_vinculada,                  # PARTE_VINCULADA
                    o_cuenta,                           # CUENTA
                    o_cuenta_pgc,                       # CUENTA_PGC
                    o_ceco,                             # CECO
                    o_coco,                             # COCO
                    o_razon_social,                     # RAZON_SOCIAL
                    o_cif_nif,                          # CIF_NIF
                    o_descripcion,                      # DESCRIPCION
                    format_f(o_margen, decimals=2),     # MARGEN
                    format_f(o_gasto, decimals=2),      # GASTO
                    format_f(o_ingreso, decimals=2),    # INGRESO
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
