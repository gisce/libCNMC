# -*- coding: utf-8 -*-
import unittest
import os
import sys
import types

sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(os.path.dirname(__file__), '..', 'cir_8_2021')
    )
)
if 'pyproj' not in sys.modules:
    pyproj = types.ModuleType('pyproj')
    pyproj.CRS = lambda value: value

    class FakeTransformer(object):
        @classmethod
        def from_crs(cls, source, target):
            return cls()

        def transform(self, *args):
            return args

    pyproj.Transformer = FakeTransformer
    sys.modules['pyproj'] = pyproj

if 'ooop' not in sys.modules:
    ooop = types.ModuleType('ooop')

    class FakeOOOP(object):
        pass

    ooop.OOOP = FakeOOOP
    sys.modules['ooop'] = ooop

from FA4 import FA4


# ──────────────────────────────────────────────
# Domain evaluator
# ──────────────────────────────────────────────

def _get_field_value(record, field_path):
    parts = field_path.split('.')
    current = record
    for part in parts:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            return None
    return current


def _eval_condition(record, cond):
    field_path, operator, value = cond
    actual = _get_field_value(record, field_path)
    if isinstance(actual, dict) and 'id' in actual:
        actual = actual['id']
    if operator == '=':
        return actual == value
    elif operator == '!=':
        return actual != value
    elif operator == '>=':
        if actual is None or value is None:
            return False
        return actual >= value
    elif operator == '<=':
        if actual is None or value is None:
            return False
        return actual <= value
    elif operator == '>':
        if actual is None or value is None:
            return False
        return actual > value
    elif operator == '<':
        if actual is None or value is None:
            return False
        return actual < value
    elif operator == 'in':
        return actual in value
    elif operator == 'not ilike':
        if actual is None or actual is False:
            return True
        pattern = value.replace('%', '').lower()
        return pattern not in actual.lower()
    elif operator == 'ilike':
        if actual is None or actual is False:
            return False
        pattern = value.replace('%', '').lower()
        return pattern in actual.lower()
    return False


def _eval_domain(record, domain, start=0):
    if start >= len(domain):
        return (True, start)

    token = domain[start]

    if isinstance(token, tuple):
        return (_eval_condition(record, token), start + 1)

    if token == '|':
        left, i = _eval_domain(record, domain, start + 1)
        right, i = _eval_domain(record, domain, i)
        return (left or right, i)
    elif token == '&':
        left, i = _eval_domain(record, domain, start + 1)
        right, i = _eval_domain(record, domain, i)
        return (left and right, i)
    elif token == '!':
        cond, i = _eval_domain(record, domain, start + 1)
        return (not cond, i)

    return (True, start + 1)


def _eval_record(record, domain):
    result = True
    idx = 0
    while idx < len(domain):
        r, idx = _eval_domain(record, domain, idx)
        result = result and r
    return result


# ──────────────────────────────────────────────
# Fake models and connection
# ──────────────────────────────────────────────

class FakeModel(object):
    def __init__(self, records):
        self._records = records
        self.search_calls = []
        self.read_calls = []

    def search(self, domain, offset=0, limit=None, order=False,
               context=None):
        self.search_calls.append((domain, offset, limit, order, context))
        result = [r for r in self._records if _eval_record(r, domain)]
        if limit not in (None, 0):
            result = result[:limit]
        return [r['id'] for r in result[offset:]]

    def read(self, ids, fields=None):
        if fields is None:
            fields = []
        self.read_calls.append((ids, fields))
        result = []
        for r in self._records:
            if r['id'] not in ids:
                continue
            row = {}
            for f in fields:
                val = r.get(f) if f in r else _get_field_value(r, f)
                if isinstance(val, dict) and 'id' in val:
                    row[f] = (val['id'],)
                elif isinstance(val, list):
                    row[f] = val
                elif isinstance(val, tuple):
                    row[f] = val
                else:
                    row[f] = val
            row['id'] = r['id']
            result.append(row)
        return result


class FakeConnection(object):
    def __init__(self, records=None):
        if records is None:
            records = {}
        all_models = (
            'ResConfig',
            'GiscedataPolissaModcontractual',
            'GiscedataCupsPs',
            'GiscedataCupsEstadistiques',
            'GiscedataPolissa',
            'ResMunicipi',
            'ResCatastraElement',
        )
        for model_name in all_models:
            recs = records.get(model_name, [])
            setattr(self, model_name, FakeModel(recs))


# ──────────────────────────────────────────────
# Test helpers
# ──────────────────────────────────────────────

VALID_POLISSA_STATES = [
    'tall', 'activa', 'baixa', 'modcontractual', 'impagament']


def make_modcon(id, data_inici, data_final, tarifa_name='2.0A',
                polissa_state='activa'):
    return {
        'id': id,
        'data_inici': data_inici,
        'data_final': data_final,
        'tarifa': {'id': id, 'name': tarifa_name},
        'polissa_id': {'id': id, 'state': polissa_state},
    }


def make_cups(id, active=True, data_baixa=None, polissa_polissa=False,
              polisses=None):
    if polisses is None:
        polisses = []
    return {
        'id': id,
        'active': active,
        'data_baixa': data_baixa,
        'polissa_polissa': polissa_polissa,
        'polisses': polisses,
    }


def make_estadistica(id, cups_id, data_vigencia):
    return {
        'id': id,
        'cups_id': {'id': cups_id},
        'data_vigencia': data_vigencia,
    }


# ──────────────────────────────────────────────
# Tests FA4
# ──────────────────────────────────────────────

class TestFormA4(unittest.TestCase):
    maxDiff = None

    def mk_form(self, records=None):
        if records is None:
            records = {}
        conn = FakeConnection(records)
        return FA4(
            connection=conn,
            codi_r1='R1-TEST',
            year=2023,
            quiet=True,
        )

    # ── Font 1: CUPS actives ──────────────────

    def test_actiu_amb_modcon_any_sencer(self):
        """CUPS actiu amb modcon que cobreix tot l'any → apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[101]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2023-01-01', '2023-12-31'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [1])

    def test_actiu_sense_modcon(self):
        """CUPS actiu sense cap modcon a l'any → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[101]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2022-01-01', '2022-12-31'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    def test_actiu_amb_modcon_estat_invalid(self):
        """CUPS actiu modcon amb polissa en estat no vàlid → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[101]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2023-01-01', '2023-12-31',
                            polissa_state='esborrany'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    def test_actiu_amb_modcon_tarifa_re(self):
        """Modcon amb tarifa que conté RE → exclòs"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[101]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2023-01-01', '2023-12-31',
                            tarifa_name='6.1BRE'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    # ── Font 1: CUPS baixades enguany ─────────

    def test_baixada_enguany_amb_vigencia(self):
        """Baixa enguany + modcon + data_vigencia >= 01-01 → apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, data_baixa='2023-06-15',
                          polisses=[101]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2023-01-01', '2023-06-30'),
            ],
            'GiscedataCupsEstadistiques': [
                make_estadistica(1, 1, '2023-06-15'),
            ],
        })
        self.assertEqual(form.get_sequence(), [1])

    def test_baixada_enguany_sense_vigencia(self):
        """Baixa enguany + modcon + sense data_vigencia → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, data_baixa='2023-06-15',
                          polisses=[101]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2023-01-01', '2023-06-30'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    # ── Font 2: CUPS sense pòlissa ─────────────

    def test_sense_polissa_amb_vigencia(self):
        """polissa_polissa=False + data_vigencia >= 01-01 → apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, polissa_polissa=False,
                          polisses=[101]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2023-03-01', '2023-06-30'),
            ],
            'GiscedataCupsEstadistiques': [
                make_estadistica(1, 1, '2023-03-01'),
            ],
        })
        self.assertEqual(form.get_sequence(), [1])

    def test_sense_polissa_sense_vigencia(self):
        """polissa_polissa=False + sense data_vigencia → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, polissa_polissa=False,
                          polisses=[101]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2023-03-01', '2023-06-30'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    def test_actiu_sense_polissa_amb_vigencia_no_apareix(self):
        """CUPS actiu sense pòlissa (polissa_polissa=False)
        amb vigència → NO apareix (font 2 és només per baixa)"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polissa_polissa=False,
                          polisses=[]),
            ],
            'GiscedataPolissaModcontractual': [],
            'GiscedataCupsEstadistiques': [
                make_estadistica(1, 1, '2023-06-15'),
            ],
        })
        self.assertEqual(form.get_sequence(), [])

    def test_baixa_sense_modcon_any_amb_vigencia_no_apareix(self):
        """CUPS baixat, polissa_polissa=False, amb vigència,
        però sense cap modcon a l'any de report → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, data_baixa='2022-06-15',
                          polissa_polissa=False, polisses=[101]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2022-01-01', '2022-12-31'),
            ],
            'GiscedataCupsEstadistiques': [
                make_estadistica(1, 1, '2023-06-15'),
            ],
        })
        self.assertEqual(form.get_sequence(), [])

    # ── Combinacions ───────────────────────────

    def test_actiu_i_baixa_amb_vigencia(self):
        """CUPS actiu + CUPS baixat amb vigència → tots dos apareixen"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[101]),
                make_cups(2, active=False, data_baixa='2023-06-15',
                          polisses=[102]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2023-01-01', '2023-12-31'),
                make_modcon(102, '2023-01-01', '2023-12-31'),
            ],
            'GiscedataCupsEstadistiques': [
                make_estadistica(1, 2, '2023-06-15'),
            ],
        })
        self.assertEqual(sorted(form.get_sequence()), [1, 2])

    def test_baixa_sense_vigencia_no_afecta_actiu(self):
        """Baixa sense vigència exclosa, actiu encara apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[101]),
                make_cups(2, active=False, data_baixa='2023-06-15',
                          polisses=[102]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(101, '2023-01-01', '2023-12-31'),
                make_modcon(102, '2023-01-01', '2023-12-31'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [1])


if __name__ == '__main__':
    unittest.main()
