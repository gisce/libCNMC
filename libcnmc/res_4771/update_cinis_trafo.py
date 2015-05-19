# -*- coding: utf-8 -*-
from libcnmc.core import UpdateFile

class UpdateCINISTrafo(UpdateFile):
    def __init__(self, **kwargs):
        super(UpdateCINISTrafo, self).__init__(**kwargs)
        self.header = [
            'num_transformador', 'cini'
        ]
        self.search_keys = [('num_transformador', 'name')]
        self.object = self.connection.GiscedataTransformadorTrafo
