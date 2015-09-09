# -*- coding: utf-8 -*-
from dateutil.parser import parse
from datetime import datetime

from libcnmc.res_4603.create_celles import CreateCelles

class CreateCelles4_2015(CreateCelles):
    def __init__(self, **kwargs):
        super(CreateCelles4_2015, self).__init__(**kwargs)
        self.header += ['cnmc_tipo_instalacion', 'data_pm']
        self.fields_read_ct += ['data_pm']
        self.fields_read_at_tram += ['data_pm']

    def build_vals(self, values):
        vals = super(CreateCelles4_2015, self).build_vals(values)
        ct_name = False
        suport_name = False
        for val in zip(self.header, values):
            if val[0] == 'installacio':
                model, name = val[1].split(',')
                model = model.lower()
                if 'ct' in model:
                    ct_name = name
                elif 'suport' in model:
                    suport_name = name
                else:
                    raise
            elif val[0] == 'data_pm':
                vals['bloquejar_pm'] = True
                if val[1] == 'auto':
                    if ct_name:
                        tipus = 'ct'
                        name = ct_name
                    if suport_name:
                        tipus = 'suport'
                        name = suport_name
                    vals[val[0]] = self.get_value(
                        tipus, name, 'data_pm')
                else:
                    date = parse(val[1])
                    vals[val[0]] = date.strftime('%Y-%m-%d')
        return vals

