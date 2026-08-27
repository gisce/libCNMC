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

from FA2 import FA2

class FakeModel(object):
    def __init__(self, search_result=None, read_result=None):
        self.search_result = search_result or []
        self.read_result = read_result or []
        self.search_calls = []
        self.read_calls = []

    def search(self, domain, *args):
        self.search_calls.append((domain, args))
        if callable(self.search_result):
            return self.search_result(domain, *args)
        return self.search_result

    def read(self, ids, fields, *args):
        self.read_calls.append((ids, fields))
        if callable(self.read_result):
            return self.read_result(ids, fields)
        return self.read_result

class FakeConnection(object):
    def __init__(self):
        self.GiscedataRe = FakeModel()
        self.GiscedataReUprs = FakeModel()
        def autoconsum_search(domain, *args):
            if ('participant_id', '!=', 84) in domain:
                return [202]
            return [201, 202]

        self.GiscedataAutoconsum = FakeModel(search_result=autoconsum_search)
        def generador_search(domain, *args):
            autoconsum_ids = dict(
                (operator, value)
                for field, operator, value in domain
                if field == 'autoconsum_id'
            ).get('in', [])
            return [autoconsum_id + 100 for autoconsum_id in autoconsum_ids]

        self.GiscedataAutoconsumGenerador = FakeModel(search_result=generador_search)

        
        # We need to simulate the finding of participant
        def read_company(ids, fields, *args):
            # Company 1 has partner_id 42
            if 1 in ids:
                return [{'id': 1, 'partner_id': [42, 'Company Partner']}]
            return []
            
        def read_company_wrapper(ids, fields, *args):
            if type(ids) is int:
                return {'partner_id': [42, 'Company Partner']}
            return read_company(ids, fields)

        def company_search(domain, *args):
            if ('codi_r1', '=', 'R1-TEST') in domain:
                return [1]
            return []

        self.ResCompany = FakeModel(search_result=company_search, read_result=read_company_wrapper)
        
        def participant_search(domain, *args):
            # Return participant ID 84 for partner 42
            if ('partner_id', '=', 42) in domain:
                return [84]
            return []
            
        self.GiscemiscParticipant = FakeModel()
        self.GiscemiscParticipant.search = participant_search

class TestFormA2(unittest.TestCase):
    def get_form(self):
        return FA2(
            connection=FakeConnection(),
            codi_r1='R1-TEST',
            year=2023,
            quiet=True
        )

    def test_get_sequence_excludes_autoconsums_by_resolved_participant(self):
        form = self.get_form()
        
        sequence = form.get_sequence()
        
        self.assertNotIn('gac.301', sequence)
        self.assertIn('gac.302', sequence)
        
        # Verify that participant_id was resolved and used in the search
        search_calls = form.connection.GiscedataAutoconsum.search_calls
        self.assertEqual(len(search_calls), 1)
        domain, args = search_calls[0]
        
        # Participant ID resolved via GiscemiscParticipant mock which returns [84]
        self.assertIn(('participant_id', '!=', 84), domain)
        
        # Check that it filters collective self-consumption correctly
        self.assertIn(('collectiu', '=', True), domain)

    def test_get_sequence_keeps_previous_behavior_if_no_participant_found(self):
        form = self.get_form()
        # Mock participant search to return empty list
        form.connection.GiscemiscParticipant.search = lambda domain, *args: []
        
        sequence = form.get_sequence()
        self.assertIn('gac.301', sequence)
        self.assertIn('gac.302', sequence)

    def test_get_sequence_keeps_previous_behavior_if_participant_is_ambiguous(self):
        form = self.get_form()
        # Mock participant search to return multiple IDs
        form.connection.GiscemiscParticipant.search = lambda domain, *args: [84, 85]
        
        sequence = form.get_sequence()
        self.assertIn('gac.301', sequence)
        self.assertIn('gac.302', sequence)

    def test_get_sequence_keeps_previous_behavior_if_no_company_found(self):
        form = self.get_form()
        form.connection.ResCompany.search = lambda domain, *args: []

        sequence = form.get_sequence()
        self.assertIn('gac.301', sequence)
        self.assertIn('gac.302', sequence)

    def test_get_sequence_keeps_previous_behavior_if_company_is_ambiguous(self):
        form = self.get_form()
        form.connection.ResCompany.search = lambda domain, *args: [1, 2]

        sequence = form.get_sequence()
        self.assertIn('gac.301', sequence)
        self.assertIn('gac.302', sequence)

if __name__ == '__main__':
    unittest.main()
