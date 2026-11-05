# -*- coding: utf-8 -*-
import unittest
from libcnmc.utils import get_stats_values


class FakeModel(object):
    def __init__(self, fields=None, records=None):
        self._fields = set(fields or [])
        self._records = records or []
        self.read_calls = []
        self.search_calls = []

    def fields_get(self, *args, **kwargs):
        # 'id' i 'cups_id' sempre presents (com a l'ORM)
        result = {f: {} for f in self._fields}
        result.setdefault('id', {})
        result.setdefault('cups_id', {})
        return result

    def search(self, domain):
        self.search_calls.append(domain)
        cups_ids = domain[0][2]
        year = domain[1][2]
        result_ids = []

        for rec in self._records:
            c_id_val = rec.get('cups_id')
            if not c_id_val:
                continue

            c_id = c_id_val[0] if isinstance(c_id_val, (list, tuple)) else c_id_val
            if c_id in cups_ids and rec.get('year') == year:
                result_ids.append(rec['id'])

        return result_ids

    def read(self, ids, fields):
        # L'ORM real no peta en camps inexistents: els omet silenciosament
        self.read_calls.append((list(ids), list(fields)))
        result = []
        for rec in self._records:
            if rec['id'] in ids:
                row = {'id': rec['id']}
                for f in fields:
                    if f in rec:
                        row[f] = rec[f]
                result.append(row)
        return result


class FakeConnection(object):
    def __init__(self, cups_fields=None, stats_fields=None,
                 cups_records=None, stats_records=None):
        self.GiscedataCupsPs = FakeModel(
            fields=cups_fields, records=cups_records)
        self.GiscedataCupsEstadistiques = FakeModel(
            fields=stats_fields, records=stats_records)


class TestGetStatsValues(unittest.TestCase):

    def test_sense_estadistiques(self):
        """Si no hi ha estadístiques de l'year, retorna directament els valors del CUPS"""
        cups_records = [
            {'id': 1, 'potencia_adscrita': 3.3, 'cne_anual_activa': 1000}]
        conn = FakeConnection(
            cups_fields={'potencia_adscrita', 'cne_anual_activa'},
            stats_fields=set(),
            cups_records=cups_records, stats_records=[])

        requested = ['potencia_adscrita', 'cne_anual_activa']
        res = get_stats_values(conn, [1], 2025, requested)

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['potencia_adscrita'], 3.3)
        self.assertEqual(res[0]['cne_anual_activa'], 1000)

        self.assertEqual(len(conn.GiscedataCupsEstadistiques.read_calls), 0)

    def test_amb_estadistiques_sobreescriu(self):
        """Si hi ha estadístiques, els seus valors trepitgen els del CUPS per aquells camps demanats"""
        cups_records = [
            {'id': 1, 'potencia_adscrita': 3.3, 'cne_anual_activa': 1000}]
        stats_records = [
            {'id': 14798, 'cups_id': 1, 'year': 2025, 'cne_anual_activa': 1200}
        ]
        conn = FakeConnection(
            cups_fields={'potencia_adscrita', 'cne_anual_activa'},
            stats_fields={'cne_anual_activa'},
            cups_records=cups_records, stats_records=stats_records)

        requested = ['potencia_adscrita', 'cne_anual_activa']
        res = get_stats_values(conn, [1], 2025, requested)

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['potencia_adscrita'], 3.3)
        self.assertEqual(res[0]['cne_anual_activa'], 1200)

    def test_multipels_cups_un_amb_estadistiques_un_sense(self):
        cups_records = [
            {'id': 1, 'potencia_adscrita': 3.3},
            {'id': 2, 'potencia_adscrita': 4.4}
        ]
        stats_records = [
            {'id': 991, 'cups_id': 2, 'year': 2025, 'potencia_adscrita': 6.0}
        ]
        conn = FakeConnection(
            cups_fields={'potencia_adscrita'},
            stats_fields={'potencia_adscrita'},
            cups_records=cups_records, stats_records=stats_records)

        res = get_stats_values(conn, [1, 2], 2025, ['potencia_adscrita'])

        res_sorted = sorted(res, key=lambda x: x['id'])
        self.assertEqual(res_sorted[0]['id'], 1)
        self.assertEqual(res_sorted[0]['potencia_adscrita'], 3.3)
        self.assertEqual(res_sorted[1]['id'], 2)
        self.assertEqual(res_sorted[1]['potencia_adscrita'], 6.0)

    def test_format_tupla_cups_id(self):
        cups_records = [{'id': 1, 'cne_anual_activa': 500}]
        stats_records = [
            {'id': 14798, 'cups_id': [1, 'ES0345000000000001BR0F'], 'year': 2025,
             'cne_anual_activa': 800
             }
        ]
        conn = FakeConnection(
            cups_fields={'cne_anual_activa'},
            stats_fields={'cne_anual_activa'},
            cups_records=cups_records, stats_records=stats_records)

        res = get_stats_values(conn, [1], 2025, ['cne_anual_activa'])

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['cne_anual_activa'], 800)

    def test_camp_no_existent_en_lloc(self):
        cups_records = [{'id': 1, 'potencia_adscrita': 3.3}]
        stats_records = [{'id': 14798, 'cups_id': 1, 'year': 2025}]
        conn = FakeConnection(
            cups_fields={'potencia_adscrita'},
            stats_fields=set(),
            cups_records=cups_records, stats_records=stats_records)

        res = get_stats_values(conn, [1], 2025, ['potencia_adscrita',
                                                 'cnmc_energia_autoconsumida'])

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['potencia_adscrita'], 3.3)
        self.assertNotIn('cnmc_energia_autoconsumida', res[0])

    def test_read_cups_only(self):
        """Camp que nomes existeix a cups es llegeix de cups, no de stats."""
        cups_records = [{'id': 1, 'cne_anual_activa': 1000}]
        stats_records = []
        conn = FakeConnection(
            cups_fields={'cne_anual_activa'},
            stats_fields=set(),
            cups_records=cups_records, stats_records=stats_records)

        res = get_stats_values(conn, [1], 2025, ['cne_anual_activa'])

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['cne_anual_activa'], 1000)
        # No s'ha demanat el camp a stats
        for call in conn.GiscedataCupsEstadistiques.read_calls:
            self.assertNotIn('cne_anual_activa', call[1])

    def test_read_stats_only(self):
        """Camp que nomes existeix a stats es llegeix de stats, no de cups."""
        cups_records = [{'id': 1}]
        stats_records = [
            {'id': 14798, 'cups_id': 1, 'year': 2025, 'cne_anual_reactiva_generada': 7.0}
        ]
        conn = FakeConnection(
            cups_fields=set(),
            stats_fields={'cne_anual_reactiva_generada'},
            cups_records=cups_records, stats_records=stats_records)

        res = get_stats_values(conn, [1], 2025, ['cne_anual_reactiva_generada'])

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['cne_anual_reactiva_generada'], 7.0)
        # cups no ha rebut el camp
        for call in conn.GiscedataCupsPs.read_calls:
            self.assertNotIn('cne_anual_reactiva_generada', call[1])

    def test_stats_only_field_absent_when_no_stats_row(self):
        """Camp nomes a stats sense fila d'estadistica: absent del resultat, sense error."""
        cups_records = [{'id': 1}]
        stats_records = []
        conn = FakeConnection(
            cups_fields=set(),
            stats_fields={'cne_anual_reactiva_generada'},
            cups_records=cups_records, stats_records=stats_records)

        res = get_stats_values(conn, [1], 2025, ['cne_anual_reactiva_generada'])

        self.assertEqual(len(res), 1)
        self.assertNotIn('cne_anual_reactiva_generada', res[0])


if __name__ == '__main__':
    unittest.main()
