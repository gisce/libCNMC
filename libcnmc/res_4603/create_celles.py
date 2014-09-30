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

    def build_vals(self, values):
        vals = {}
        o = self.connection
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
                        ct_id = self.cts[name]
                    else:
                        ct_id = o.GiscedataCts.search(
                            [('name', '=', name)],
                            0, 0, False,
                            {'active_test': False})[0]
                        self.cts[name] = {'id': ct_id}
                    value = '%s,%s' % ('giscedata.cts', ct_id)

                elif 'suport' in model:
                    if name in self.at_suports:
                        at_suport_id = self.at_suports[name]
                    else:
                        at_suport_id = o.GiscedataAtSuport.search(
                            [('name', '=', name)], 0, 0, False,
                            {'active_test': False})[0]
                        self.at_suports[name] = at_suport_id
                    value = '%s,%s' % ('giscedata.at.suport', at_suport_id)

                vals[val[0]] = value
            else:
                vals[val[0]] = val[1]
        return vals

