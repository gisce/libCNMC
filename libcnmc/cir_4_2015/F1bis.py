# -*- coding: utf-8 -*-
from datetime import datetime, timedelta
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
        self.modcons_in_year = set(mods_fi + mods_ini + mod_all_year)

    def get_sequence(self):
        """
        Searches all the CUPS that where create after 01/01/actual year

        :return: List of id of CUPS
        :rtype: list of int
        """

        data_ini = '%s-01-01' % (self.year + 1)
        search_params = [('active', '=', True),
                         '|',
                         ('create_date', '<', data_ini),
                         ('create_date', '=', False)]
        return self.connection.GiscedataCupsPs.search(
            search_params, 0, 0, False, {'active_test': False})

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
        """

        :param cups_id:
        :return:
        """
        O = self.connection
        polissa_obj = O.GiscedataPolissa

        data_inici = '%s-01-01' % self.year
        data_fi = '%s-12-31' % self.year

        # Busquem pòlisses que s'han donat de baixa durant l'any
        search_params = [
            ('cups', '=', cups_id),
            ('data_baixa', '>=', data_inici),
            ('data_baixa', '<', data_fi)
        ]
        polisses_baixa_ids = polissa_obj.search(
            search_params, 0, False, False, {'active_test': False})

        # Agafem les dates de baixa de les pòlisses
        data_format = '%Y-%m-%d'
        dates_baixa = [
            polissa['data_baixa'] for polissa
            in polissa_obj.read(polisses_baixa_ids, ['data_baixa'])
        ]

        # Sumem un dia a cada data de baixa
        dates_alta = [
            (
                datetime.strptime(data_baixa, data_format) + timedelta(days=1)
            ).strftime(data_format)
            for data_baixa in dates_baixa
        ]

        # Busquem pòlisses on la data d'alta estigui en les dates d'alta
        polisses_alta_ids = polissa_obj.search(
            [('cups', '=', cups_id), ('data_alta', 'in', dates_alta)],
            0, False, False, {'active_test': False}
        )

        # Si hi ha el mateix número de pòlisses que s'han donat de baixa durant
        # l'any i pòlisses que s'han donat d'alta al dia següent llavors '0',
        # altrament '1'
        if len(polisses_baixa_ids) == len(polisses_alta_ids):
            return '0'
        else:
            return '1'

    def consumer(self):
        O = self.connection
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                fields_to_read = [
                    'name', 'polissa_polissa', 'cnmc_numero_lectures',
                    "polisses"
                ]
                cups = O.GiscedataCupsPs.read(item, fields_to_read)
                o_cups = cups['name'][:22]
                if not set(cups["polisses"]).intersection(self.modcons_in_year):
                    continue
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
