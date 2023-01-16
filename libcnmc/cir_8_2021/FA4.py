# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased
from libcnmc.utils import TARIFAS_BT, TARIFAS_AT


class FA4(MultiprocessBased):
    def __init__(self, **kwargs):
        super(FA4, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.reducir_cups = kwargs.get("reducir_cups", False)
        self.report_name = 'FA4 - CUPS-CT'
        self.base_object = 'CTS i CUPS'
        mod_all_year = self.connection.GiscedataPolissaModcontractual.search(
            [
                ("data_inici", "<=", "{}-01-01".format(self.year)),
                ("data_final", ">=", "{}-12-31".format(self.year)),
                ("tarifa.name", 'not ilike', '%RE%'),
                ('polissa_id.state', 'in', ['tall', 'activa', 'baixa'])
            ], 0, 0, False, {"active_test": False}
                                )
        mods_ini = self.connection.GiscedataPolissaModcontractual.search(
            [
                ("data_inici", ">=", "{}-01-01".format(self.year)),
                ("data_inici", "<=", "{}-12-31".format(self.year)),
                ("tarifa.name", 'not ilike', '%RE%'),
                ('polissa_id.state', 'in', ['tall', 'activa', 'baixa'])
            ], 0, 0, False, {"active_test": False}
        )
        mods_fi = self.connection.GiscedataPolissaModcontractual.search(
            [
                ("data_final", ">=", "{}-01-01".format(self.year)),
                ("data_final", "<=", "{}-12-31".format(self.year)),
                ("tarifa.name", 'not ilike', '%RE%'),
                ('polissa_id.state', 'in', ['tall', 'activa', 'baixa'])
            ], 0, 0, False, {"active_test": False}
        )

        self.generate_derechos = kwargs.pop("derechos", False)
        self.modcons_in_year = set(mods_fi + mods_ini + mod_all_year)

    def get_derechos(self, tarifas, years):
        """
        Returns a list of CUPS with derechos

        :param tarifas: Lis of tarifas of the polissas that are in the CUPS
        :param years: Number of years to search back
        :return: List of ids of the
        """

        O = self.connection

        polisses_baixa_id = O.GiscedataPolissa.search(
            [
                ("data_baixa", "<=", "{}-12-31".format(self.year - 1)),
                ("data_baixa", ">", "{}-01-01".format(self.year - years)),
                ("tarifa", "in", tarifas)
            ],
            0, 0, False, {'active_test': False}
        )

        cups_polisses_baixa = [x["cups"][0] for x in O.GiscedataPolissa.read(
            polisses_baixa_id, ["cups"]
        )]

        cups_derechos = O.GiscedataCupsPs.search(
            [
                ("id", "in", cups_polisses_baixa),
                ("polissa_polissa", "=", False)
            ]
        )

        polissa_eliminar_id = O.GiscedataPolissaModcontractual.search(
            [
                ("cups", "in", cups_derechos),
                '|', ("data_inici", ">", "{}-01-01".format(self.year)),
                ("data_final", ">", "{}-01-01".format(self.year))
            ],
            0, 0, False, {'active_test': False}
        )

        cups_eliminar_id = [x["cups"][0] for x in
                            O.GiscedataPolissaModcontractual.read(
                                polissa_eliminar_id, ["cups"]
                            )]

        cups_derechos = list(set(cups_derechos) - set(cups_eliminar_id))

        return cups_derechos

    def get_sequence(self):
        data_ini = '%s-01-01' % (self.year + 1)
        # data_baixa = '%s-12-31' % self.year
        search_params = [('active', '=', True),
                         '|',
                         ('create_date', '<', data_ini),
                         ('create_date', '=', False)]

        ret_cups_ids = self.connection.GiscedataCupsPs.search(
            search_params, 0, 0, False, {'active_test': False})

        ret_cups_tmp = self.connection.GiscedataCupsPs.read(
            ret_cups_ids, ["polisses"]
        )
        ret_cups = []

        for cups in ret_cups_tmp:
            if set(cups['polisses']).intersection(self.modcons_in_year):
                ret_cups.append(cups["id"])

        if self.generate_derechos:
            cups_derechos_bt = self.get_derechos(TARIFAS_BT, 2)
            cups_derechos_at = self.get_derechos(TARIFAS_AT, 4)
            return list(set(ret_cups + cups_derechos_at + cups_derechos_bt))
        else:
            return ret_cups

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
        """
        Consumer function that executes for each value returned by get_sequence

        :return: None
        """

        o = self.connection
        fields_to_read = [
            'name', 'et', 'polisses', 'id'
        ]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                cups = o.GiscedataCupsPs.read(
                    item, fields_to_read
                )

                if self.reducir_cups:
                    o_cups = cups['name'][:20]
                else:
                    o_cups = cups['name']
                o_cini = self.get_cini(cups['et'])
                if not o_cini:
                    o_cini = 'False'
                o_codi_ct = cups['et']
                self.output_q.put([
                    o_cups,         # CUPS
                    o_cini,         # CINI
                    o_codi_ct       # CODIGO SUBESTACION
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
