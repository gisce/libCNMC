# -*- coding: utf-8 -*-
from datetime import datetime
import unittest
from libcnmc.core import UpdateCNMCStats


class FakeModel(object):
    def __init__(self, search_func=None, write_func=None, ensure_year_func=None):
        self._search = search_func
        self._write = write_func
        self._ensure_year = ensure_year_func

    def search(self, domain):
        return self._search(domain) if self._search else []

    def ensure_year(self, year, cups_ids=None):
        if self._ensure_year:
            self._ensure_year(year, cups_ids)

    def write(self, ids, vals):
        if self._write:
            self._write(ids, vals)


class FakeConnection(object):
    pass


class TestUpdateCNMCStats(unittest.TestCase):
    def setUp(self):
        self.records_stats = {
            101: {'cups_id': 1, 'year': 2023, 'cne_anual_activa': 100.0},
            102: {'cups_id': 1, 'year': 2024, 'cne_anual_activa': 0.0},
        }
        self.calls_cups = []
        self.calls_stats = []

        self.conn = FakeConnection()
        self.conn.GiscedataCupsPs = FakeModel(search_func=self.search_cups)
        self.conn.GiscedataCupsEstadistiques = FakeModel(
            search_func=self.search_stats,
            write_func=self.write_stats,
            ensure_year_func=self.ensure_year_stats
        )

    def search_cups(self, domain):
        self.calls_cups.append(('search', domain))
        if domain == [('name', '=', 'ES0001000000000001SP')]:
            return [1]
        return []

    def search_stats(self, domain):
        self.calls_stats.append(('search', domain))
        d = dict((term[0], term[2]) for term in domain)
        return [
            rec_id for rec_id, rec in self.records_stats.items()
            if rec['cups_id'] in d.get('cups_id', []) and rec['year'] == d.get('year')
        ]

    def ensure_year_stats(self, year, cups_ids=None):
        self.calls_stats.append(('ensure_year', year, cups_ids))

    def write_stats(self, ids, vals):
        self.calls_stats.append(('write', ids, vals))
        for rec_id in ids:
            self.records_stats[rec_id].update(vals)

    def test_proxy_and_year_parameter(self):
        updater = UpdateCNMCStats(connection=self.conn, file_input='fake.csv', year=2024)
        self.assertEqual(updater.object, self.conn.GiscedataCupsEstadistiques)
        self.assertNotEqual(updater.object, self.conn.GiscedataCupsPs)
        self.assertEqual(updater.year, 2024)

        updater_default = UpdateCNMCStats(connection=self.conn, file_input='fake.csv')
        self.assertEqual(updater_default.year, datetime.today().year - 1)

    def test_search_and_update_year_isolation(self):
        updater = UpdateCNMCStats(connection=self.conn, file_input='fake.csv', year=2024)
        vals = {
            'cups': 'ES0001000000000001SP',
            'cne_anual_activa': 500.0,
        }
        updater.search_and_update(vals)

        self.assertEqual(
            self.calls_cups,
            [('search', [('name', '=', 'ES0001000000000001SP')])]
        )

        self.assertEqual(
            self.calls_stats,
            [
                ('ensure_year', 2024, [1]),
                ('search', [('cups_id', 'in', [1]), ('year', '=', 2024)]),
                ('write', [102], {'cne_anual_activa': 500.0}),
            ]
        )

        self.assertEqual(self.records_stats[102]['cne_anual_activa'], 500.0)
        self.assertEqual(self.records_stats[101]['cne_anual_activa'], 100.0)

    def test_search_and_update_cups_inexistent(self):
        updater = UpdateCNMCStats(connection=self.conn, file_input='fake.csv', year=2024)
        updater.search_and_update({'cups': 'ES_NO_EXISTEIX', 'cne_anual_activa': 999.0})

        self.assertEqual(len(self.calls_stats), 0)


if __name__ == '__main__':
    unittest.main()