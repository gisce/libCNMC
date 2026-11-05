# -*- coding: utf-8 -*-
from datetime import datetime
import unittest
from libcnmc.core import UpdateCNMCStats


CNMC_FIELDS = [
    'cne_anual_activa', 'cne_anual_reactiva',
    'cnmc_potencia_facturada', 'cnmc_numero_lectures',
    'cne_anual_activa_generada', 'cne_anual_reactiva_generada',
    'cnmc_energia_autoconsumida', 'cnmc_energia_excedentaria',
    'cnmc_factures_total', 'cnmc_factures_estimades',
]


class FakeModel(object):
    def __init__(self, fields=None, search_func=None, write_func=None,
                 ensure_year_func=None):
        self._fields = set(fields or [])
        self._search = search_func
        self._write = write_func
        self._ensure_year = ensure_year_func
        self.write_calls = []
        self.ensure_year_calls = []
        self.search_calls = []

    def fields_get(self, *args, **kwargs):
        return {f: {} for f in self._fields}

    def search(self, domain):
        self.search_calls.append(domain)
        return self._search(domain) if self._search else []

    def ensure_year(self, year, cups_ids=None):
        self.ensure_year_calls.append((year, cups_ids))
        if self._ensure_year:
            self._ensure_year(year, cups_ids)

    def write(self, ids, vals):
        self.write_calls.append((list(ids), dict(vals)))
        if self._write:
            self._write(ids, vals)


class FakeConnection(object):
    pass


def make_conn(cups_fields, stats_fields, cups_search=None, stats_search=None,
              stats_write=None, stats_ensure_year=None):
    conn = FakeConnection()
    conn.GiscedataCupsPs = FakeModel(
        fields=cups_fields, search_func=cups_search)
    conn.GiscedataCupsEstadistiques = FakeModel(
        fields=stats_fields, search_func=stats_search,
        write_func=stats_write, ensure_year_func=stats_ensure_year)
    return conn


class TestUpdateCNMCStats(unittest.TestCase):
    def setUp(self):
        self.records_stats = {
            101: {'cups_id': 1, 'year': 2023, 'cne_anual_activa': 100.0},
            102: {'cups_id': 1, 'year': 2024, 'cne_anual_activa': 0.0},
        }
        self.calls_cups = []
        self.calls_stats = []

        def search_cups(domain):
            self.calls_cups.append(('search', domain))
            if domain == [('name', '=', 'ES0001000000000001SP')]:
                return [1]
            return []

        def search_stats(domain):
            self.calls_stats.append(('search', domain))
            d = dict((term[0], term[2]) for term in domain)
            return [
                rec_id for rec_id, rec in self.records_stats.items()
                if rec['cups_id'] in d.get('cups_id', []) and rec['year'] == d.get('year')
            ]

        def ensure_year_stats(year, cups_ids=None):
            self.calls_stats.append(('ensure_year', year, cups_ids))

        def write_stats(ids, vals):
            self.calls_stats.append(('write', ids, vals))
            for rec_id in ids:
                self.records_stats[rec_id].update(vals)

        # Default layout: cne_anual_activa migrated to stats, rest on cups
        stats_fields = {'cne_anual_activa'}
        cups_fields = set(CNMC_FIELDS) - stats_fields
        self.conn = make_conn(
            cups_fields=cups_fields,
            stats_fields=stats_fields,
            cups_search=search_cups,
            stats_search=search_stats,
            stats_write=write_stats,
            stats_ensure_year=ensure_year_stats,
        )
        # keep references for assertions in some tests
        self._search_cups = search_cups
        self._search_stats = search_stats
        self._write_stats = write_stats
        self._ensure_year_stats = ensure_year_stats

    def _vals(self, extra=None):
        vals = {'cups': 'ES0001000000000001SP'}
        for f in CNMC_FIELDS:
            vals[f] = 500.0 if f == 'cne_anual_activa' else 1.0
        if extra:
            vals.update(extra)
        return vals

    def test_proxy_and_year_parameter(self):
        updater = UpdateCNMCStats(connection=self.conn, file_input='fake.csv', year=2024)
        self.assertEqual(updater.object, self.conn.GiscedataCupsEstadistiques)
        self.assertNotEqual(updater.object, self.conn.GiscedataCupsPs)
        self.assertEqual(updater.year, 2024)

        updater_default = UpdateCNMCStats(connection=self.conn, file_input='fake.csv')
        self.assertEqual(updater_default.year, datetime.today().year - 1)

    def test_search_and_update_year_isolation(self):
        updater = UpdateCNMCStats(connection=self.conn, file_input='fake.csv', year=2024)
        vals = {'cups': 'ES0001000000000001SP', 'cne_anual_activa': 500.0}
        updater.search_and_update(vals)

        self.assertEqual(
            self.conn.GiscedataCupsPs.search_calls,
            [[('name', '=', 'ES0001000000000001SP')]]
        )

        # ensure_year + search + write on stats for the migrated field
        self.assertEqual(
            self.conn.GiscedataCupsEstadistiques.ensure_year_calls,
            [(2024, [1])]
        )
        self.assertIn(
            [('cups_id', 'in', [1]), ('year', '=', 2024)],
            self.conn.GiscedataCupsEstadistiques.search_calls
        )
        self.assertEqual(
            self.conn.GiscedataCupsEstadistiques.write_calls,
            [([102], {'cne_anual_activa': 500.0})]
        )

        self.assertEqual(self.records_stats[102]['cne_anual_activa'], 500.0)
        self.assertEqual(self.records_stats[101]['cne_anual_activa'], 100.0)

    def test_search_and_update_cups_inexistent(self):
        updater = UpdateCNMCStats(connection=self.conn, file_input='fake.csv', year=2024)
        updater.search_and_update({'cups': 'ES_NO_EXISTEIX', 'cne_anual_activa': 999.0})

        self.assertEqual(len(self.conn.GiscedataCupsEstadistiques.write_calls), 0)
        self.assertEqual(len(self.conn.GiscedataCupsEstadistiques.ensure_year_calls), 0)

    def test_write_only_cups_when_no_cnmc_in_stats(self):
        """Estat developer: cap camp CNMC a estadistiques -> tot a cups, sense ensure_year."""
        cups_fields = set(CNMC_FIELDS)
        stats_fields = set()
        conn = make_conn(
            cups_fields=cups_fields,
            stats_fields=stats_fields,
            cups_search=lambda d: [1] if d == [('name', '=', 'ES0001000000000001SP')] else [],
        )
        updater = UpdateCNMCStats(connection=conn, file_input='fake.csv', year=2024)
        vals = self._vals()
        vals['cups'] = 'ES0001000000000001SP'
        updater.search_and_update(vals)

        self.assertEqual(len(conn.GiscedataCupsEstadistiques.write_calls), 0)
        self.assertEqual(len(conn.GiscedataCupsEstadistiques.ensure_year_calls), 0)
        self.assertEqual(len(conn.GiscedataCupsPs.write_calls), 1)
        written_vals = conn.GiscedataCupsPs.write_calls[0][1]
        for f in CNMC_FIELDS:
            self.assertIn(f, written_vals)

    def test_split_moved_column_to_stats(self):
        """Estat #28966: cne_anual_reactiva_generada a stats, la resta a cups."""
        stats_fields = {'cne_anual_reactiva_generada'}
        cups_fields = set(CNMC_FIELDS) - stats_fields
        conn = make_conn(
            cups_fields=cups_fields,
            stats_fields=stats_fields,
            cups_search=lambda d: [1] if d == [('name', '=', 'ES0001000000000001SP')] else [],
            stats_search=lambda d: [102],
        )
        updater = UpdateCNMCStats(connection=conn, file_input='fake.csv', year=2024)
        vals = self._vals()
        vals['cups'] = 'ES0001000000000001SP'
        updater.search_and_update(vals)

        self.assertEqual(conn.GiscedataCupsEstadistiques.ensure_year_calls, [(2024, [1])])
        self.assertEqual(len(conn.GiscedataCupsEstadistiques.write_calls), 1)
        self.assertEqual(
            conn.GiscedataCupsEstadistiques.write_calls[0][1],
            {'cne_anual_reactiva_generada': 1.0}
        )
        self.assertEqual(len(conn.GiscedataCupsPs.write_calls), 1)
        cups_written = conn.GiscedataCupsPs.write_calls[0][1]
        self.assertNotIn('cne_anual_reactiva_generada', cups_written)
        for f in cups_fields:
            self.assertIn(f, cups_written)

    def test_fully_migrated(self):
        """Tots els camps a stats: tot s'escriu a stats, res a cups."""
        stats_fields = set(CNMC_FIELDS)
        cups_fields = set()
        conn = make_conn(
            cups_fields=cups_fields,
            stats_fields=stats_fields,
            cups_search=lambda d: [1] if d == [('name', '=', 'ES0001000000000001SP')] else [],
            stats_search=lambda d: [102],
        )
        updater = UpdateCNMCStats(connection=conn, file_input='fake.csv', year=2024)
        vals = self._vals()
        vals['cups'] = 'ES0001000000000001SP'
        updater.search_and_update(vals)

        self.assertEqual(conn.GiscedataCupsEstadistiques.ensure_year_calls, [(2024, [1])])
        self.assertEqual(len(conn.GiscedataCupsEstadistiques.write_calls), 1)
        stats_written = conn.GiscedataCupsEstadistiques.write_calls[0][1]
        for f in CNMC_FIELDS:
            self.assertIn(f, stats_written)
        self.assertEqual(len(conn.GiscedataCupsPs.write_calls), 0)

    def test_overlap_logs_and_skips(self):
        """Un camp present als dos models: log i s'escriu a estadistiques."""
        overlap_field = 'cne_anual_activa'
        stats_fields = {overlap_field}
        cups_fields = set(CNMC_FIELDS)  # overlap_field present a cups també
        conn = make_conn(
            cups_fields=cups_fields,
            stats_fields=stats_fields,
            cups_search=lambda d: [1] if d == [('name', '=', 'ES0001000000000001SP')] else [],
            stats_search=lambda d: [102],
        )
        updater = UpdateCNMCStats(connection=conn, file_input='fake.csv', year=2024)
        vals = self._vals()
        vals['cups'] = 'ES0001000000000001SP'
        updater.search_and_update(vals)
        # El camp solapat NO s'escriu a cap model (nomes advarteix)
        for w in conn.GiscedataCupsEstadistiques.write_calls:
            self.assertNotIn(overlap_field, w[1])
        for w in conn.GiscedataCupsPs.write_calls:
            self.assertNotIn(overlap_field, w[1])

    def test_missing_logs_and_skips(self):
        """Un camp absent als dos models: log i s'omet, la resta s'escriu."""
        missing_field = 'cne_anual_activa'
        stats_fields = set()
        cups_fields = set(CNMC_FIELDS) - {missing_field}
        conn = make_conn(
            cups_fields=cups_fields,
            stats_fields=stats_fields,
            cups_search=lambda d: [1] if d == [('name', '=', 'ES0001000000000001SP')] else [],
        )
        updater = UpdateCNMCStats(connection=conn, file_input='fake.csv', year=2024)
        vals = self._vals()
        vals['cups'] = 'ES0001000000000001SP'
        updater.search_and_update(vals)
        # no raise; cups write no inclou el camp absent
        cups_written = conn.GiscedataCupsPs.write_calls[0][1] if conn.GiscedataCupsPs.write_calls else {}
        self.assertNotIn(missing_field, cups_written)


if __name__ == '__main__':
    unittest.main()
