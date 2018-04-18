# -*- coding: utf-8 -*-
from datetime import datetime
import traceback

from libcnmc.utils import get_comptador, format_f
from libcnmc.core import MultiprocessBased


class F1bis(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F1bis, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F1bis - CUPS'
        self.base_object = 'CUPS'
        self.ultim_dia_any = '{}-12-31'.format(self.year)
        mod_all_year = self.connection.GiscedataPolissaModcontractual.search([
            ("data_inici", "<=", "{}-01-01".format(self.year)),
            ("data_final", ">=", "{}-12-31".format(self.year))],
            0, 0, False,{"active_test": False}
        )
        mods_ini = self.connection.GiscedataPolissaModcontractual.search(
            [("data_inici", ">=", "{}-01-01".format(self.year)),
            ("data_inici", "<=", "{}-12-31".format(self.year))],
            0, 0, False,{"active_test": False}
        )
        mods_fi = self.connection.GiscedataPolissaModcontractual.search(
            [("data_final", ">=", "{}-01-01".format(self.year)),
            ("data_final", "<=", "{}-12-31".format(self.year))],
            0,0,False,{"active_test": False}
        )
        self.generate_derechos = True
        self.modcons_in_year = set(mods_fi + mods_ini + mod_all_year)

    def get_derechos_bt(self):
        """
        Returns the CUPS ids with derecho BT

        :return: List of CUPS ids
        """
        O = self.connection

        bt_tarifas = ['2.0A', '2.0DHA', '2.1A', '2.1DHA', '2.0DHS',
                      '2.1DHS', '3.0A']

        polisses_baixa_id = O.GiscedataPolissa.search(
            [
                ("data_baixa", "<=", "2016-12-31"),
                ("data_baixa", ">", "2015-01-01"),
                ("tarifa", "in", bt_tarifas)
            ],
            0, 0, False, {'active_test': False}
        )

        cups_polisses_baixa = [x["cups"][0] for x in O.GiscedataPolissa.read(
            polisses_baixa_id, ["cups"]
        )]

        cups_derechos_bt = O.GiscedataCupsPs.search(
            [
                ("id", "in", cups_polisses_baixa),
                ("polissa_polissa", "=", False)
            ]
        )

        polissa_eliminar_id = O.GiscedataPolissaModcontractual.search(
            [
                ("cups", "in", cups_derechos_bt),
                '|', ("data_inici", ">", "2017-01-01"),
                ("data_final", ">", "2017-01-01")
            ],
            0, 0, False, {'active_test': False}
        )

        cups_eliminar_id = [x["cups"][0] for x in O.GiscedataPolissaModcontractual.read(
            polissa_eliminar_id, ["cups"]
        )]

        cups_derechos_bt = list(set(cups_derechos_bt) - set(cups_eliminar_id))

        return cups_derechos_bt

    def get_derechos_at(self):
        """
        Returns the CUPS ids with derechos AT

        :return: List of CUPS ids
        """

        at_tarifas = ['3.1A', '3.1A LB', '6.1', '6.1A',
                      '6.1B', '6.2', '6.2A', '6.2B']

        O = self.connection

        polisses_baixa_id = O.GiscedataPolissa.search(
            [
                ("data_baixa", "<=", "2016-12-31"),
                ("data_baixa", ">", "2015-01-01"),
                ("tarifa", "in", at_tarifas)
            ],
            0, 0, False, {'active_test': False}
        )

        cups_polisses_baixa = [x["cups"][0] for x in O.GiscedataPolissa.read(
            polisses_baixa_id, ["cups"]
        )]

        cups_derechos_at = O.GiscedataCupsPs.search(
            [
                ("id", "in", cups_polisses_baixa),
                ("polissa_polissa", "=", False)
            ]
        )

        polissa_eliminar_id = O.GiscedataPolissaModcontractual.search(
            [
                ("cups", "in", cups_derechos_at),
                '|', ("data_inici", ">", "2017-01-01"),
                ("data_final", ">", "2017-01-01")
            ],
            0, 0, False, {'active_test': False}
        )

        cups_eliminar_id = [x["cups"][0] for x in
                            O.GiscedataPolissaModcontractual.read(
                                polissa_eliminar_id, ["cups"]
                            )]

        cups_derechos_at = list(set(cups_derechos_at) - set(cups_eliminar_id))

        return cups_derechos_at

    def get_sequence(self):
        """
        Searches all the CUPS that where create after 01/01/actual year

        :return: List of id of CUPS
        :rtype: list of int
        """
        o = self.connection
        data_ini = '%s-01-01' % (self.year + 1)
        search_params = [('active', '=', True),
                         '|',
                         ('create_date', '<', data_ini),
                         ('create_date', '=', False)]

        cups_derechos_bt = self.get_derechos_bt()
        cups_derechos_at = self.get_derechos_at()

        tmp_ret_cups = self.connection.GiscedataCupsPs.search(
            search_params, 0, 0, False, {'active_test': False})
        ret_cups = []

        fields_to_check = ["polisses"]

        for cups in o.GiscedataCupsPs.read(tmp_ret_cups, fields_to_check):
            if set(cups["polisses"]).intersection(self.modcons_in_year):
                ret_cups.append(cups["id"])

        if self.generate_derechos:
            return ret_cups + cups_derechos_at + cups_derechos_bt
        else:
            return ret_cups

    def get_comptador(self, polissa_id):
        o = self.connection
        comp_obj = o.GiscedataLecturesComptador
        comp_id = comp_obj.search([
            ('polissa', '=', polissa_id),
            ('data_alta', '<', '%s-01-01' % (self.year + 1))
        ], 0, 1, 'data_alta desc', {'active_test': False})
        return comp_id

    def get_comptador_cini(self, polissa_id):
        comp_obj = self.connection.GiscedataLecturesComptador
        cid = get_comptador(self.connection, polissa_id, self.year)
        cini = ''
        if cid:
            comptador = comp_obj.read(cid[0], ['cini'])
            cini = comptador['cini'] or ''
        return cini

    def get_data_comptador(self, polissa_id):
        comp_obj = self.connection.GiscedataLecturesComptador
        comp_id = get_comptador(self.connection, polissa_id, self.year)
        data = ''
        if comp_id:
            comp_id = comp_id[0]
            comp = comp_obj.read(comp_id, ['data_alta'])
            data = comp['data_alta'].split('-')
            # Format MM/YYYY
            data = '%s/%s' % (data[1], data[0])
        return data

    def get_polissa(self, cups_id):
        polissa_obj = self.connection.GiscedataPolissa
        context = {
            'date': self.ultim_dia_any,
            'active_test': False
        }
        polissa_id = polissa_obj.search([
            ('cups', '=', cups_id),
            ('state', 'not in', ('esborrany', 'validar')),
            ('data_alta', '<=', self.ultim_dia_any),
            # '|',
        ], 0, 1, 'data_alta desc', context)
        return polissa_id

    def get_cambio_titularidad(self, cups_id):
        O = self.connection
        intervals = O.GiscedataCupsPs.get_modcontractual_intervals
        start = '%s-01-01' % self.year
        end = '%s-12-31' % self.year
        modcons = intervals(cups_id, start, end, {'ffields': ['titular']})
        if len(modcons) > 1:
            return '1'
        else:
            return '0'

    def get_baixa_cups(self, cups_id):
        O = self.connection
        intervals = O.GiscedataCupsPs.get_modcontractual_intervals
        start = '%s-01-01' % self.year
        end = '%s-12-31' % self.year
        modcons = intervals(cups_id, start, end, {'ffields': ['titular']})
        dates = sorted([tuple(x['dates']) for x in modcons.values()])
        days = 1
        for idx, interval in enumerate(dates):
            if idx + 1 == len(dates):
                break
            end = datetime.strptime(interval[-1], '%Y-%m-%d')
            start = datetime.strptime(dates[idx + 1][0], '%Y-%m-%d')
            days = max((start - end).days, days)
        if days > 1:
            return '1'
        else:
            return '0'

    def consumer(self):
        """
        Consumer function that executes for each item of the return
        of get_sequence

        :return: None
        """
        O = self.connection
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                fields_to_read = [
                    'name', 'polissa_polissa', 'cnmc_numero_lectures',
                ]
                cups = O.GiscedataCupsPs.read(item, fields_to_read)
                o_cups = cups['name'][:22]
                polissa_id = self.get_polissa(cups['id'])
                polissa = O.GiscedataPolissa.read(polissa_id[0], ['tarifa'])
                if 'RE' in polissa['tarifa'][1]:
                    continue
                if polissa_id:
                    polissa_id = polissa_id[0]
                    o_comptador_cini = self.get_comptador_cini(polissa_id)
                    o_comptador_data = self.get_data_comptador(polissa_id)
                else:
                    o_comptador_cini = ''
                    o_comptador_data = ''
                o_num_lectures = format_f(
                    cups['cnmc_numero_lectures'], decimals=3) or '0'
                o_titular = self.get_cambio_titularidad(cups['id'])
                o_baixa = self.get_baixa_cups(cups['id'])
                o_year = self.year

                self.output_q.put([
                    o_cups,
                    o_comptador_cini,
                    o_comptador_data,
                    o_num_lectures,
                    o_titular,
                    o_baixa,
                    o_year
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
