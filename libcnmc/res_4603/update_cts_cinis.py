# -*- coding: utf-8 -*-
from libcnmc.core import UpdateFile


class UpdateCINISCts(UpdateFile):
    def __init__(self, **kwargs):
        super(UpdateCINISCts, self).__init__(**kwargs)
        self.header = [
            'num_cts', 'cini'
        ]
        self.search_keys = [('num_cts', 'name')]
        self.object = self.connection.GiscedataCts
