# -*- coding: utf-8 -*-
from datetime import datetime
from multiprocessing import Manager
import traceback

from libcnmc.core import MultiprocessBased

resultats = {}


class F3(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F3, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        manager = Manager()
        self.cts = manager.dict()
        self.base_object = 'Clients'
        self.report_name = 'F3 - Clients'

    def get_sequence(self):
        data = '%s-12-31' % self.year
        search_params = ['&',
                         ('data_inici', '<=', data),
                         ('data_final', '>=', data)]
        ids = self.connection.GiscedataPolissaModcontractual.search(
            search_params, 0, 0, False, {'active_test': False})
        for item in ids:
            self.escriure(item, 'comer')
            distri = self.get_distri()
        self.escriure(distri, 'distri')
        res = []
        for elem in resultats:
            tupla = (elem, resultats[elem])
            res.append(tupla)
        return res

    def get_distri(self):
        o = self.connection
        res = None
        id_distri = o.ResCompany.search([])
        distri = o.ResCompany.read(id_distri, ['partner_id'])
        if distri:
            res = distri[0]['partner_id'][0]
        return res

    def escriure(self, item, mode):
        fields_to_read_modcon = [
            'polissa_id'
        ]
        fields_to_read_polissa = [
            'comercialitzadora', 'name'
        ]
        fields_to_read_comer = [
            'vat', 'name', 'ref2',
        ]
        o = self.connection
        modcon = o.GiscedataPolissaModcontractual.read(
            item, fields_to_read_modcon)
        comer_id = None
        if modcon and mode == 'comer':
            polissa = o.GiscedataPolissa.read(
                modcon['polissa_id'], fields_to_read_polissa)
            if polissa:
                comercialitzadora = polissa[0]['comercialitzadora'][0]
                comer_id = o.ResPartner.search(
                    [('id', '=', comercialitzadora)])[0]
        elif mode == 'distri':
            comer_id = item
        if comer_id:
            comer = o.ResPartner.read(
                comer_id, fields_to_read_comer)
            if comer and mode != 'distri':
                if comer['name'] not in resultats.keys():
                    resultats[comer['name']] = {}
                    resultats[comer['name']]['total_clients'] = 0
                    resultats[comer['name']]['vat'] = comer['vat']
                    resultats[comer['name']]['ref2'] = comer['ref2']
                resultats[comer['name']]['total_clients'] += 1
            elif comer and mode == 'distri':
                self.output_q.put([
                    comer['vat'],
                    comer['name'],
                    comer['ref2'],
                    self.get_total_clients(resultats),
                    0,
                    0,
                    self.year
                ])

    def get_total_clients(self, resultats):
        res = 0
        for elem in resultats:
            res += resultats[elem]['total_clients']
        return res

    def consumer(self):
        # generar linies

        while True:
            item = self.input_q.get()
            self.progress_q.put(item)
            try:
                self.output_q.put([
                    item[1]['vat'],
                    item[0],
                    item[1]['ref2'],
                    item[1]['total_clients'],
                    0,
                    0,
                    self.year
                ])
            except Exception, e:
                print e
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
