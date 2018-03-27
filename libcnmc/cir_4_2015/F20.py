# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased


class F20(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F20, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.report_name = 'F20 - CTS'
        self.base_object = 'CTS'
        mod_all_year = self.connection.GiscedataPolissaModcontractual.search([
                ("data_inici", "<=", "{}-01-01".format(self.year)),
                ("data_final", ">=", "{}-12-31".format(self.year))],
                0, 0, False, {"active_test": False}
                                )
        mods_ini = self.connection.GiscedataPolissaModcontractual.search(
                [("data_inici", ">=", "{}-01-01".format(self.year)),
                 ("data_inici", "<=", "{}-12-31".format(self.year))],
                0, 0, False, {"active_test": False}
        )
        mods_fi = self.connection.GiscedataPolissaModcontractual.search(
                [("data_final", ">=", "{}-01-01".format(self.year)),
                 ("data_final", "<=", "{}-12-31".format(self.year))],
                0, 0, False, {"active_test": False}
        )
        self.modcons_in_year = set(mods_fi + mods_ini + mod_all_year)

    def get_sequence(self):
        data_ini = '%s-01-01' % (self.year + 1)
        # data_baixa = '%s-12-31' % self.year
        search_params = [('active', '=', True),
                         '|',
                         ('create_date', '<', data_ini),
                         ('create_date', '=', False)]
        return self.connection.GiscedataCupsPs.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_cini(self, et):
        o = self.connection
        valor = ''
        if et:
            cts = o.GiscedataCts.search([('name', '=', et)])
            if cts:
                cini = o.GiscedataCts.read(cts[0], ['cini'])
                valor = cini['cini']
        return valor

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'name', 'et', 'polisses'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                cups = o.GiscedataCupsPs.read(
                    item, fields_to_read
                )
                if not set(cups['polisses']).intersection(self.modcons_in_year):
                    continue
                o_codi_r1 = "R1-"+self.codi_r1
                o_cups = cups['name']
                o_cini = self.get_cini(cups['et'])
                if not o_cini:
                    o_cini = 'False'
                o_codi_ct = cups['et']
                self.output_q.put([
                    o_codi_r1,
                    o_cups,
                    o_cini,
                    o_codi_ct
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
