# coding=utf-8
from unittest.mock import MagicMock, patch
from mamba import description, context, it, before
from expects import expect, equal

from libcnmc.cir_8_2021.FB3_3 import FB3_3


with description('FB3_3'):
    with description('process_condensador'):
        with before.each:
            self.fb3_3 = FB3_3.__new__(FB3_3)
            self.fb3_3.connection = MagicMock()

        with context('when operacio is set to "1" (Operatiu)'):
            with it('returns the operacio value from the condensador'):
                condensador_data = {
                    'ct_id': [1, 'SE-001'],
                    'name': 'COND-001',
                    'cini': 'I00001AB',
                    'parc_alta': [10, 'PARC-A'],
                    'parc_baixa': [20, 'PARC-B'],
                    'data_pm': '2010-03-15',
                    'operacio': '1',
                }
                self.fb3_3.connection.GiscedataCondensadors.read.return_value = (
                    condensador_data
                )
                result = self.fb3_3.process_condensador(1)
                expect(result[6]).to(equal('1'))

        with context('when operacio is set to "0" (Reserva freda)'):
            with it('returns "0"'):
                condensador_data = {
                    'ct_id': [1, 'SE-001'],
                    'name': 'COND-001',
                    'cini': 'I00001AB',
                    'parc_alta': False,
                    'parc_baixa': False,
                    'data_pm': '2015-06-01',
                    'operacio': '0',
                }
                self.fb3_3.connection.GiscedataCondensadors.read.return_value = (
                    condensador_data
                )
                result = self.fb3_3.process_condensador(1)
                expect(result[6]).to(equal('0'))

        with context('when operacio is not set (None/False)'):
            with it('defaults to "1" (Operatiu)'):
                condensador_data = {
                    'ct_id': [1, 'SE-001'],
                    'name': 'COND-002',
                    'cini': 'I00002CD',
                    'parc_alta': False,
                    'parc_baixa': False,
                    'data_pm': False,
                    'operacio': False,
                }
                self.fb3_3.connection.GiscedataCondensadors.read.return_value = (
                    condensador_data
                )
                result = self.fb3_3.process_condensador(1)
                expect(result[6]).to(equal('1'))

        with context('when data_pm is set'):
            with it('extracts the year from data_pm'):
                condensador_data = {
                    'ct_id': [1, 'SE-001'],
                    'name': 'COND-003',
                    'cini': 'I00003EF',
                    'parc_alta': False,
                    'parc_baixa': False,
                    'data_pm': '2018-07-22',
                    'operacio': '1',
                }
                self.fb3_3.connection.GiscedataCondensadors.read.return_value = (
                    condensador_data
                )
                result = self.fb3_3.process_condensador(1)
                expect(result[5]).to(equal('2018'))
