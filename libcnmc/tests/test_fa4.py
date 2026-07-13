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


class FakeModel(object):
    def __init__(self, search_result=None, read_result=None):
        self.search_result = search_result or []
        self.read_result = read_result or []
        self.search_calls = []
        self.read_calls = []

    def search(self, domain, *args):
        self.search_calls.append((domain, args))
        return self.search_result

    def read(self, ids, fields):
        self.read_calls.append((ids, fields))
        return self.read_result


class FakeConnection(object):
    def __init__(self):
        self.GiscedataPolissaModcontractual = FakeModel(
            search_result=[101, 102]
        )
        self.GiscedataCupsPs = FakeModel(
            search_result=[1, 2, 3],
            read_result=[
                {'id': 1, 'polisses': [101]},
                {'id': 2, 'polisses': [999]},
                {'id': 3, 'polisses': [102]},
            ]
        )


class TestFormA4(unittest.TestCase):
    def get_form(self):
        return FA4(
            connection=FakeConnection(),
            codi_r1='R1-TEST',
            year=2023,
            quiet=True
        )

    def test_get_sequence_considers_all_cups_with_active_test_false(self):
        form = self.get_form()

        sequence = form.get_sequence()

        self.assertEqual(sequence, [1, 3])
        self.assertEqual(
            form.connection.GiscedataCupsPs.search_calls,
            [([], (0, 0, False, {'active_test': False}))]
        )
        self.assertEqual(
            form.connection.GiscedataCupsPs.read_calls,
            [([1, 2, 3], ['polisses'])]
        )

    def test_modcontractual_search_uses_a1_polissa_states(self):
        form = self.get_form()
        mod_model = form.connection.GiscedataPolissaModcontractual

        self.assertEqual(len(mod_model.search_calls), 3)
        for domain, args in mod_model.search_calls:
            self.assertIn(
                ('polissa_id.state', 'in', [
                    'tall', 'activa', 'baixa',
                    'modcontractual', 'impagament'
                ]),
                domain
            )
            self.assertEqual(args, (0, 0, False, {'active_test': False}))


if __name__ == '__main__':
    unittest.main()
