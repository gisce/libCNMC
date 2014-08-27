# -*- coding: utf-8 -*-
import unittest
import traceback

from ooop import OOOP
from libcnmc.cir_4_2014.F1 import F1
import test_f1_config

class TestFormF1(unittest.TestCase):
    def test_f1(self):
        O = OOOP(dbname=test_f1_config.DB, user=test_f1_config.USER,
             pwd=test_f1_config.PWD, port=test_f1_config.PORT,
             uri=test_f1_config.URI)
        f1 = F1(
            output=test_f1_config.OUTPUT,
            connection=O,
            codi_r1=test_f1_config.CODI_R1,
            year=2013,
            quiet=False
        )
        f1.calc()
        self.assertTrue(True)

    def test_codi_tarifa(self):
        def check_even(codi):
            self.assertTrue(codi)
        O = OOOP(dbname=test_f1_config.DB, user=test_f1_config.USER,
             pwd=test_f1_config.PWD, port=test_f1_config.PORT,
             uri=test_f1_config.URI)
        f1 = F1(
            output=test_f1_config.OUTPUT,
            connection=O,
            codi_r1=test_f1_config.CODI_R1,
            year=2013,
            quiet=False
        )
        ultim_dia_any = '%s-12-31' % f1.year
        search_glob = [
            ('state', 'not in', ('esborrany', 'validar')),
            ('data_alta', '<=', ultim_dia_any),
            '|',
            ('data_baixa', '>=', ultim_dia_any),
            ('data_baixa', '=', False)
        ]
        context_glob = {'date': ultim_dia_any, 'active_test': False}
        cups_id = f1.get_sequence()
        search_params = [('cups', 'in', cups_id)] + search_glob
        polissa_ids = O.GiscedataPolissa.search(search_params, 0, 0, False,
                                                context_glob)
        raised = False
        tarifes_error = {}
        for tarifa in O.GiscedataPolissa.read(polissa_ids, ['tarifa']):
            try:
                f1.get_codi_tarifa(tarifa['tarifa'][1])
            except:
                if tarifa['tarifa'][1] not in tarifes_error:
                    tarifes_error[tarifa['tarifa'][1]] = 0
                else:
                    tarifes_error[tarifa['tarifa'][1]] += 1
                #traceback.print_exc()
                raised = True
        print tarifes_error
        self.assertFalse(raised)