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

from libcnmc.utils import TEMPORAL_POLISSA_STATES
from libcnmc.cir_8_2021 import FA1

# ──────────────────────────────────────────────
# Domain evaluador
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
    elif operator == 'not in':
        if actual is None or actual is False:
            return True
        return actual not in value
    return False


def _eval_domain(record, domain, start=0):
    """
    Avalua recursivament un domini OpenERP en prefix.
    Retorna (resultat, proper_index).
    """
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
    """Avalua un domini sencer contra un registre.
    Les condicions de nivell superior s'ANDegen implÃ­citament."""
    result = True
    idx = 0
    while idx < len(domain):
        r, idx = _eval_domain(record, domain, idx)
        result = result and r
    return result


# ──────────────────────────────────────────────
# Models i connexiÃ³ falsa
# ──────────────────────────────────────────────

class FakeModel(object):
    """
    Emmagatzema registres i suporta search/read
    amb avaluaciÃ³ real de dominis.
    """

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

    def add_record(self, record):
        self._records.append(record)


class FakeConnection(object):
    """
    ConnexiÃ³ falsa que exposa models ERP amb FakeModel.
    """

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
# Helpers de test
# ──────────────────────────────────────────────

VALID_POLISSA_STATES = [
    'tall', 'activa', 'baixa', 'modcontractual', 'impagament']


def make_modcon(id, data_inici, data_final, tarifa_name='2.0A',
                polissa_state='activa', contract_type='01'):
    return {
        'id': id,
        'data_inici': data_inici,
        'data_final': data_final,
        'tarifa': {'id': id, 'name': tarifa_name},
        'polissa_id': {'id': id, 'state': polissa_state},
        'contract_type': contract_type,
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
# Tests FA1
# ──────────────────────────────────────────────

class TestFormA1(unittest.TestCase):
    maxDiff = None

    def mk_form(self, records=None, generate_derechos=False):
        if records is None:
            records = {}
        conn = FakeConnection(records)
        return FA1(
            connection=conn,
            codi_r1='R1-TEST',
            year=2023,
            quiet=True,
            derechos=generate_derechos,
        )

    # ────────────────────────────────────────
    # Font 1: CUPS actives
    # ────────────────────────────────────────

    def test_actiu_amb_modcon_any_sencer(self):
        """CUPS actiu amb modcon que cobreix tot l'any → apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[201]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-01-01', '2023-12-31'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [1])

    def test_actiu_sense_modcon_any(self):
        """CUPS actiu sense cap modcon a l'any → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[201]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2022-01-01', '2022-12-31'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    def test_actiu_amb_modcon_polissa_estat_invalid(self):
        """CUPS actiu modcon amb polissa en estat no vàlid → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[201]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-01-01', '2023-12-31',
                            polissa_state='esborrany'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    # ────────────────────────────────────────
    # Font 1: CUPS baixades enguany (data_baixa dins l'any)
    # ────────────────────────────────────────

    def test_baixada_enguany_amb_vigencia(self):
        """Baixa enguany + modcon + data_vigencia >= 01-01 → apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, data_baixa='2023-06-15',
                          polisses=[201]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-01-01', '2023-06-30'),
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
                make_cups(1, active=False, data_baixa='2022-06-15',
                          polisses=[201]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2022-01-01', '2022-06-30'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    def test_baixada_enguany_sense_modcon(self):
        """Baixa enguany sense modcon a l'any → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, data_baixa='2023-06-15',
                          polisses=[]),
            ],
            'GiscedataPolissaModcontractual': [],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    # ────────────────────────────────────────
    # Font 2: CUPS sense pòlissa (polissa_polissa=False)
    # ────────────────────────────────────────

    def test_sense_polissa_amb_vigencia(self):
        """polissa_polissa=False + data_vigencia >= 01-01 → apareix (cas ID 11)"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, polissa_polissa=False,
                          polisses=[201]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-03-01', '2023-06-30'),
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
                          polisses=[201]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-03-01', '2023-06-30'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    def test_sense_polissa_data_vigencia_any_anterior(self):
        """polissa_polissa=False + data_vigencia < 01-01 → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, polissa_polissa=False,
                          polisses=[201]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2022-06-01', '2022-12-31'),
            ],
            'GiscedataCupsEstadistiques': [
                make_estadistica(1, 1, '2022-12-31'),
            ],
        })
        self.assertEqual(form.get_sequence(), [])

    def test_amb_polissa_no_entra_per_font2(self):
        """polissa_polissa != False → NO apareix per font 2"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polissa_polissa=101,
                          polisses=[201]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-01-01', '2023-12-31'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        # Font 1 el troba (active=True + modcon)
        self.assertEqual(form.get_sequence(), [1])

    # ────────────────────────────────────────
    # Font 2: ventana data_baixa [any-6, any-1]
    # ────────────────────────────────────────

    def test_baixa_ventana_dins_amb_vigencia(self):
        """Baixat 2018 (dins ventana [2017,2022]) + vigència 2023 → apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, data_baixa='2018-06-15',
                          polissa_polissa=False, polisses=[]),
            ],
            'GiscedataPolissaModcontractual': [],
            'GiscedataCupsEstadistiques': [
                make_estadistica(1, 1, '2023-06-15'),
            ],
        })
        self.assertEqual(form.get_sequence(), [1])

    def test_baixa_ventana_limit_inferior(self):
        """Límit inferior ventana: data_baixa = any-6 → apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, data_baixa='2017-01-01',
                          polissa_polissa=False, polisses=[]),
            ],
            'GiscedataPolissaModcontractual': [],
            'GiscedataCupsEstadistiques': [
                make_estadistica(1, 1, '2023-06-15'),
            ],
        })
        self.assertEqual(form.get_sequence(), [1])

    def test_baixa_ventana_limit_superior(self):
        """Límit superior ventana: data_baixa = any-1 → apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, data_baixa='2022-12-31',
                          polissa_polissa=False, polisses=[]),
            ],
            'GiscedataPolissaModcontractual': [],
            'GiscedataCupsEstadistiques': [
                make_estadistica(1, 1, '2023-06-15'),
            ],
        })
        self.assertEqual(form.get_sequence(), [1])

    def test_baixa_ventana_sense_vigencia(self):
        """Dins ventana però sense data_vigència → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=False, data_baixa='2018-06-15',
                          polissa_polissa=False, polisses=[]),
            ],
            'GiscedataPolissaModcontractual': [],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    # ────────────────────────────────────────
    # Reactivació
    # ────────────────────────────────────────

    def test_reactivat_sense_vigencia(self):
        """Reactivada: active=True, modcon històric dins l'any,
        sense data_vigencia → apareix (no cal vigència als actius)"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polissa_polissa=101,
                          polisses=[201]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-03-01', '2023-06-30'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [1])

    # ────────────────────────────────────────
    # Combinacions
    # ────────────────────────────────────────

    def test_actiu_i_baixa_amb_vigencia(self):
        """CUPS actiu + CUPS baixat amb vigència → tots dos apareixen"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polissa_polissa=101,
                          polisses=[201]),
                make_cups(2, active=False, data_baixa='2022-06-15',
                          polissa_polissa=False,
                          polisses=[202]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-01-01', '2023-12-31'),
                make_modcon(202, '2023-01-01', '2023-12-31'),
            ],
            'GiscedataCupsEstadistiques': [
                make_estadistica(1, 2, '2029-06-15'),
            ],
        })
        self.assertEqual(sorted(form.get_sequence()), [1, 2])

    def test_baixa_sense_vigencia_no_afecta_actiu(self):
        """Baixa sense vigència exclosa, actiu encara apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polissa_polissa=101,
                          polisses=[201]),
                make_cups(2, active=False, data_baixa='2022-06-15',
                          polissa_polissa=False,
                          polisses=[202]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-01-01', '2023-12-31'),
                make_modcon(202, '2022-01-01', '2022-12-31'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [1])

    # ────────────────────────────────────────
    # Exclusió per tipus de contracte temporal
    # (FAQ 08/06/2026, apartat 4.1.2)
    # ────────────────────────────────────────

    def test_contract_type_excluded(self):
        """CUPS amb modcon de tipus temporal → NO apareix"""
        for contract_type in TEMPORAL_POLISSA_STATES:
            form = self.mk_form({
                'GiscedataCupsPs': [
                    make_cups(1, active=True, polisses=[201]),
                ],
                'GiscedataPolissaModcontractual': [
                    make_modcon(201, '2023-01-01', '2023-12-31',
                                contract_type=contract_type),
                ],
                'GiscedataCupsEstadistiques': [],
            })
            self.assertEqual(
                form.get_sequence(), [],
                'contract_type={}'.format(contract_type)
            )

    def test_contract_type_non_excluded(self):
        """CUPS amb modcon de tipus no temporal → apareix"""
        non_excluded = ['01', '05', '08', '10', '11', '12']
        for contract_type in non_excluded:
            form = self.mk_form({
                'GiscedataCupsPs': [
                    make_cups(1, active=True, polisses=[201]),
                ],
                'GiscedataPolissaModcontractual': [
                    make_modcon(201, '2023-01-01', '2023-12-31',
                                contract_type=contract_type),
                ],
                'GiscedataCupsEstadistiques': [],
            })
            self.assertEqual(
                form.get_sequence(), [1],
                'contract_type={}'.format(contract_type)
            )

    def test_mixt_exclou_si_tots_exclouen(self):
        """CUPS amb dos modcons, ambdós exclosos → NO apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[201, 202]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-01-01', '2023-06-30',
                            contract_type='02'),
                make_modcon(202, '2023-07-01', '2023-12-31',
                            contract_type='07'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [])

    def test_mixt_inclou_si_algun_permes(self):
        """CUPS amb modcon exclòs + modcon permès → apareix"""
        form = self.mk_form({
            'GiscedataCupsPs': [
                make_cups(1, active=True, polisses=[201, 202]),
            ],
            'GiscedataPolissaModcontractual': [
                make_modcon(201, '2023-01-01', '2023-06-30',
                            contract_type='02'),
                make_modcon(202, '2023-07-01', '2023-12-31',
                            contract_type='01'),
            ],
            'GiscedataCupsEstadistiques': [],
        })
        self.assertEqual(form.get_sequence(), [1])


if __name__ == '__main__':
    unittest.main()
