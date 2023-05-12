# -*- coding: utf-8 -*-
import traceback
from datetime import datetime

from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import get_ine, format_f


class FD1(StopMultiprocessBased):
    def __init__(self, **kwargs):
        super(FD1, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = "Formulario D1: Continuidad del suministro"
        self.base_object = 'D1'

    def get_sequence(self):
        prevision_ids = self.connection.Cir82021D1.search(
            [('year', '=', self.year)]
        )
        return prevision_ids

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
                d1 = O.Cir82021D1.read(item, fields_to_read)
                o_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi(
                    d1['municipio'][0]
                )
                ine = O.ResMunicipi.read(d1['municipio'][0], ['ine'])['ine']
                o_municipio, o_provincia = get_ine(O, ine)
                self.output_q.put([
                    d1['tipo'],
                    o_municipio,
                    o_provincia,
                    o_ccaa,
                    d1['zona'],
                    format_f(d1['potencia'], 2),
                    d1['n_cups'],
                    format_f(d1['programadas_transporte'], 2),
                    format_f(d1['programadas_distribucion'], 2),
                    format_f(d1['programadas_total'], 2),
                    format_f(d1['imprevistas_generacion'], 2),
                    format_f(d1['imprevistas_transporte'], 2),
                    format_f(d1['imprevistas_terceros'], 2),
                    format_f(d1['imprevistas_fuerza_mayor'], 2),
                    format_f(d1['imprevistas_propias'], 2),
                    format_f(d1['total'], 2)
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
