# -*- coding: utf-8 -*-
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased


class resum(MultiprocessBased):

    def __init__(self, **kwargs):
        super(resum, self).__init__(**kwargs)
        self.complet = (kwargs.pop('complet') == '1')
        self.year = kwargs.pop('year')

    def get_sequence(self):
        if self.complet:
            search_params = [('anyo', 'in', [2015, 2016, 2017])]
        else:
            search_params = [('anyo', '=', 2015)]
        pla_inversio = self.connection.GiscedataCnmcPla_inversio.search(
            search_params)
        if len(pla_inversio) > 0:
            pla_inversio_id = pla_inversio[0]
        else:
            return []
        return self.connection.GiscedataCnmcResum_any.search(
            [('pla_inversio', '=', pla_inversio_id)], 0, 0, 'anyo'
        )

    def default_values(self, value, default):
        if not value:
            return default
        return value

    def consumer(self):
        O = self.connection
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                dades = O.GiscedataCnmcResum_any.read(item, [])
                if self.complet:
                    any = '%s;' % dades['anyo'] or ''
                else:
                    any = ''
                self.output_q.put(
                    [
                        '%s%s' % (any, dades['pib']) or '',
                        '%s' % dades['pib_prev'] or '',
                        '%s' % dades['limit'] or '',
                        '%s' % dades['inc_demanda'] or '',
                        '%s' % dades['limit_empresa'] or '',
                        '%s' % dades['inc_demanda_empresa'] or '',
                        '%s' % self.default_values(dades['vpi_sup'], 'no'),
                        '%s' % dades['volum_total_inv'] or '',
                        '%s' % dades['ajudes_prev'] or '',
                        '%s' % dades['financiacio'] or '',
                        '%s' % dades['n_projectes'] or '',
                        '%s' % dades['vpi_bt'] or '',
                        '%s' % self.default_values(dades['inv_ccaa'], '00'),
                        '%s' % self.default_values(dades['inf_ccaa'], 'no'),
                ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
