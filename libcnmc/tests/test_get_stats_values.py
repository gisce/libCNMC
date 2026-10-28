# -*- coding: utf-8 -*-
import unittest


# Assumim que la funció està en algun fitxer d'utilitats, per exemple:
# from libcnmc.utils import get_stats_values

# Definim la funció aquí mateix per a que el test sigui autocontingut
def get_stats_values(connection, cups_ids, year, requested_fields):
    cups_data = connection.GiscedataCupsPs.read(cups_ids, requested_fields)

    stats_ids = connection.GiscedataCupsEstadistiques.search([
        ('cups_id', 'in', cups_ids),
        ('any', '=', year)
    ]
    )

    if not stats_ids:
        return cups_data

    read_fields = requested_fields + ['cups_id']
    stats_data = connection.GiscedataCupsEstadistiques.read(stats_ids,
                                                            read_fields
                                                            )

    stats_by_cups = {}
    for stat in stats_data:
        c_id = stat.get('cups_id')
        if c_id:
            c_id_val = c_id[0] if isinstance(c_id, (list, tuple)) else c_id
            stats_by_cups[c_id_val] = stat

    for record in cups_data:
        cup_id = record['id']
        if cup_id in stats_by_cups:
            stat_record = stats_by_cups[cup_id]
            for field in requested_fields:
                if field in stat_record:
                    record[field] = stat_record[field]

    return cups_data


# ──────────────────────────────────────────────
# Models i connexió falsa per simular l'ERP
# ──────────────────────────────────────────────

class FakeModel(object):
    def __init__(self, records=None):
        self._records = records or []
        self.read_calls = []
        self.search_calls = []

    def search(self, domain):
        self.search_calls.append(domain)
        # Mock de cerca simplificat específic per [('cups_id', 'in', [...]), ('any', '=', year)]
        cups_ids = domain[0][2]
        year = domain[1][2]
        result_ids = []

        for rec in self._records:
            c_id_val = rec.get('cups_id')
            if not c_id_val:
                continue
            c_id = c_id_val[0] if isinstance(c_id_val, (list, tuple)
                                             ) else c_id_val

            if c_id in cups_ids and rec.get('any') == year:
                result_ids.append(rec['id'])

        return result_ids

    def read(self, ids, fields):
        self.read_calls.append((ids, fields))
        result = []
        for rec in self._records:
            if rec['id'] in ids:
                # Odoo sempre retorna l'ID
                row = {'id': rec['id']}
                for f in fields:
                    # Només afegim el camp si existeix al registre fals
                    if f in rec:
                        row[f] = rec[f]
                result.append(row)
        return result


class FakeConnection(object):
    def __init__(self, cups_records=None, stats_records=None):
        self.GiscedataCupsPs = FakeModel(cups_records)
        self.GiscedataCupsEstadistiques = FakeModel(stats_records)


# ──────────────────────────────────────────────
# Casos de Test
# ──────────────────────────────────────────────

class TestGetStatsValues(unittest.TestCase):

    def test_sense_estadistiques(self):
        """Si no hi ha estadístiques de l'any, retorna directament els valors del CUPS"""
        cups_records = [
            {'id': 1, 'potencia_adscrita': 3.3, 'cne_anual_activa': 1000}]
        # Estadístiques buides
        conn = FakeConnection(cups_records=cups_records, stats_records=[])

        requested = ['potencia_adscrita', 'cne_anual_activa']
        res = get_stats_values(conn, [1], 2025, requested)

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['potencia_adscrita'], 3.3)
        self.assertEqual(res[0]['cne_anual_activa'], 1000)

        # Comprovem que no s'ha fet cap read a estadístiques perquè el search ha tornat buit
        self.assertEqual(len(conn.GiscedataCupsEstadistiques.read_calls), 0)

    def test_amb_estadistiques_sobreescriu(self):
        """Si hi ha estadístiques, els seus valors trepitgen els del CUPS per aquells camps demanats"""
        cups_records = [
            {'id': 1, 'potencia_adscrita': 3.3, 'cne_anual_activa': 1000}]
        stats_records = [
            {'id': 14798, 'cups_id': 1, 'any': 2025, 'cne_anual_activa': 1200}
        ]
        conn = FakeConnection(cups_records=cups_records,
                              stats_records=stats_records
                              )

        requested = ['potencia_adscrita', 'cne_anual_activa']
        res = get_stats_values(conn, [1], 2025, requested)

        self.assertEqual(len(res), 1)
        # Es manté el valor del CUPS perquè l'estadística no en té
        self.assertEqual(res[0]['potencia_adscrita'], 3.3)
        # Es sobreescriu perquè l'estadística sí que té aquest camp
        self.assertEqual(res[0]['cne_anual_activa'], 1200)

    def test_multipels_cups_un_amb_estadistiques_un_sense(self):
        """Si demanem múltiples CUPS, es fa el creuament per als que tenen dada i per la resta no"""
        cups_records = [
            {'id': 1, 'potencia_adscrita': 3.3},
            {'id': 2, 'potencia_adscrita': 4.4}
        ]
        # Només el CUPS 2 té estadístiques pel 2025
        stats_records = [
            {'id': 991, 'cups_id': 2, 'any': 2025, 'potencia_adscrita': 6.0}
        ]
        conn = FakeConnection(cups_records=cups_records,
                              stats_records=stats_records
                              )

        res = get_stats_values(conn, [1, 2], 2025, ['potencia_adscrita'])

        # Ordenem per ID per fer bé els asserts
        res_sorted = sorted(res, key=lambda x: x['id'])
        self.assertEqual(res_sorted[0]['id'], 1)
        self.assertEqual(res_sorted[0]['potencia_adscrita'], 3.3
                         )  # Manté origen

        self.assertEqual(res_sorted[1]['id'], 2)
        self.assertEqual(res_sorted[1]['potencia_adscrita'], 6.0
                         )  # Sobreescrit per stats

    def test_format_tupla_cups_id(self):
        """Assegura que l'extracció de l'ID quan Odoo retorna list/tuple [1, 'nom'] funciona bé"""
        cups_records = [{'id': 1, 'cne_anual_activa': 500}]
        stats_records = [
            {'id': 14798, 'cups_id': [1, 'ES0345000000000001BR0F'], 'any': 2025,
             'cne_anual_activa': 800
             }
        ]
        conn = FakeConnection(cups_records=cups_records,
                              stats_records=stats_records
                              )

        res = get_stats_values(conn, [1], 2025, ['cne_anual_activa'])

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['cne_anual_activa'], 800)

    def test_camp_no_existent_en_lloc(self):
        """Un camp demanat que no està ni a CUPS ni a Estadístiques no fa petar la funció i no surt al resultat"""
        cups_records = [{'id': 1, 'potencia_adscrita': 3.3}]
        stats_records = [{'id': 14798, 'cups_id': 1, 'any': 2025}]
        conn = FakeConnection(cups_records=cups_records,
                              stats_records=stats_records
                              )

        # Demanem un camp que el FakeModel no retornarà ('cnmc_energia_autoconsumida')
        res = get_stats_values(conn, [1], 2025, ['potencia_adscrita',
                                                 'cnmc_energia_autoconsumida']
                               )

        self.assertEqual(len(res), 1)
        self.assertEqual(res[0]['potencia_adscrita'], 3.3)
        self.assertNotIn('cnmc_energia_autoconsumida', res[0])


if __name__ == '__main__':
    unittest.main()