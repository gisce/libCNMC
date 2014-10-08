# -*- coding: utf-8 -*-
from multiprocessing import Manager


from libcnmc.core import UpdateFile

class CreateCelles(UpdateFile):
    def __init__(self, **kwargs):
        super(CreateCelles, self).__init__(**kwargs)
        self.header = [
            'name', 'tipus_element', 'installacio', 'tipus_posicio',
            'inventari', 'aillament', 'cini', 'propietari', 'perc_financament',
            'tensio'
        ]
        self.search_keys = [('cups', 'name')]
        self.object = self.connection.GiscedataCellesCella
        manager = Manager()
        self.cts = manager.dict()
        self.at_suports = manager.dict()

    def model2cc(self, model):
        """Converteix el model en CamelCase.
        """
        return ''.join(map(lambda x: x.capitalize(), model.split('.')))

    def search_and_update(self, vals):
        id_cella = self.object.create(vals)

    def get_value(self, tipus, clau, camp):
        if tipus == 'ct':
            if self.cts.get(clau, {}).get(camp, False):

                return self.cts[clau][camp]
            else:
                ct_dades = self.connection.GiscedataCts.read(
                    self.cts[clau]['id'], ['perc_financament', 'propietari'])
                vals = {
                    'perc_financament':
                        ct_dades['perc_financament'],
                    'propietari': ct_dades['propietari'],
                }
                vals.update(self.cts[clau])
                self.cts[clau] = vals
                return self.cts[clau][camp]
        elif tipus == 'suport':
            if self.at_suports.get(clau, {}).get(camp, False):
                return self.at_suports[clau][camp]
            else:
                suport_dades = self.connection.GiscedataAtSuport.read(
                    self.at_suports[clau]['id'], ['linia'])
                linia_id = suport_dades['linia'][0]
                linia_dades = self.connection.GiscedataAtLinia.read(
                    linia_id, ['trams', 'propietari']
                )
                tram_dades = self.connection.GiscedataAtTram.read(
                    linia_dades['trams'][0], ['perc_financament']
                )
                vals = {
                    'perc_financament':
                        tram_dades['perc_financament'],
                    'propietari': linia_dades['propietari'],
                }
                vals.update(self.at_suports[clau])
                self.at_suports[clau] = vals
                return self.at_suports[clau][camp]

    def build_vals(self, values):
        vals = {}
        o = self.connection
        ct_name = False
        suport_name = False
        for val in zip(self.header, values):
            if val[0] == 'tipus_posicio':
                search_param = [('codi', '=', val[1])]
                value = o.GiscedataCellesTipusPosicio.search(search_param)[0]
                vals[val[0]] = value
            elif val[0] == 'tipus_element':
                search_param = [('codi', '=', val[1])]
                value = o.GiscedataCellesTipusElement.search(search_param)[0]
                vals[val[0]] = value
            elif val[0] == 'aillament':
                search_param = [('name', '=', val[1].upper())]
                value = o.GiscedataCellesAillament.search(search_param)[0]
                vals[val[0]] = value
            elif val[0] == 'tensio':
                search_param = [('name', '=', val[1].upper())]
                value = o.GiscedataTensionsTensio.search(search_param)[0]
                vals[val[0]] = value
            elif val[0] == 'installacio':
                model, name = val[1].split(',')
                model = model.lower()
                if 'ct' in model:
                    if name in self.cts:
                        ct_id = self.cts[name]['id']
                    else:
                        ct_id = o.GiscedataCts.search(
                            [('name', '=', name)],
                            0, 0, False,
                            {'active_test': False})[0]
                        self.cts[name] = {'id': ct_id}
                    value = '%s,%s' % ('giscedata.cts', ct_id)
                    ct_name = name
                elif 'suport' in model:
                    if name in self.at_suports:
                        at_suport_id = self.at_suports[name]['id']
                    else:
                        at_suport_id = o.GiscedataAtSuport.search(
                            [('name', '=', name)], 0, 0, False,
                            {'active_test': False})[0]
                        self.at_suports[name] = {'id': at_suport_id}
                    value = '%s,%s' % ('giscedata.at.suport', at_suport_id)
                    suport_name = name
                vals[val[0]] = value
            elif val[0] == 'propietari':
                if val[1] == 'auto':
                    if ct_name:
                        tipus = 'ct'
                        name = ct_name
                    if suport_name:
                        tipus = 'suport'
                        name = suport_name
                    vals[val[0]] = self.get_value(tipus, name, 'propietari')
                else:
                    vals[val[0]] = int(val[1])
            elif val[0] == 'perc_financament':
                if val[1] == 'auto':
                    if ct_name:
                        tipus = 'ct'
                        name = ct_name
                    if suport_name:
                        tipus = 'suport'
                        name = suport_name
                    vals[val[0]] = self.get_value(
                        tipus, name, 'perc_financament')
                else:
                    vals[val[0]] = val[1]
            else:
                vals[val[0]] = val[1]
        return vals

