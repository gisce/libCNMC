# -*- coding: utf-8 -*-
from erppeek import Client


fields_to_read_modcon = [
    'polissa_id'
]
fields_to_read_polissa = [
    'comercialitzadora', 'name'
]
fields_to_read_comer = [
    'vat', 'name', 'ref2',
]

resultats = {}


def f3(**kwargs):
    data = '%s-12-31' % kwargs['year']
    search_params = ['&',
                     ('data_inici', '<=', data),
                     ('data_final', '>=', data)]
    uri = "{0}:{1}".format(kwargs['server'], kwargs['port'])
    o = Client(uri, kwargs['database'], kwargs['user'],
               kwargs['password'])
    ids_polisses = o.GiscedataPolissaModcontractual.search(
        search_params, 0, 0, False, {'active_test': False})
    with open(kwargs['output'], 'w') as f:
        for item in ids_polisses:
            modcon = o.GiscedataPolissaModcontractual.read(
                item, fields_to_read_modcon)
            comer_id = None
            if modcon:
                polissa = o.GiscedataPolissa.read(
                    modcon['polissa_id'], fields_to_read_polissa)
                if polissa:
                    comercialitzadora = polissa[0]['comercialitzadora'][0]
                    comer_id = o.ResPartner.search(
                        [('id', '=', comercialitzadora)])[0]
            if comer_id:
                comer = o.ResPartner.read(
                    comer_id, fields_to_read_comer)
                if comer:
                    if comer['name'] not in resultats.keys():
                        resultats[comer['name']] = {}
                        resultats[comer['name']]['total_clients'] = 0
                        resultats[comer['name']]['vat'] = comer['vat']
                        resultats[comer['name']]['ref2'] = comer['ref2']
                    resultats[comer['name']]['total_clients'] += 1
        #ja tenim totes les comercialitzadores
        distri = get_distri(o)
        escriure_fitxer(distri, f, kwargs['year'])
        escriure_fitxer(resultats, f, kwargs['year'])


def get_distri(o):
    res = None
    id_distri = o.ResCompany.search([])
    distri = o.ResCompany.read(id_distri, ['partner_id'])
    if distri:
        res = distri[0]['partner_id'][0]
    dades_distri = o.ResPartner.read(
        res, fields_to_read_comer)
    dict_distri = {}
    dict_distri[dades_distri['name']] = {}
    dict_distri[dades_distri['name']]['total_clients'] = get_total_clients()
    dict_distri[dades_distri['name']]['vat'] = dades_distri['vat']
    dict_distri[dades_distri['name']]['ref2'] = dades_distri['ref2']
    return dict_distri


def get_total_clients():
    res = 0
    for elem in resultats:
        res += resultats[elem]['total_clients']
    return res


def escriure_fitxer(diccionari, f, any):
    for elem in diccionari:
        linia = "{0};{1};{2};{3};{4};{5};{6}\n".format(
            diccionari[elem]['vat'], elem, diccionari[elem]['ref2'],
            diccionari[elem]['total_clients'], 0, 0, any
        )
        f.write(linia)
