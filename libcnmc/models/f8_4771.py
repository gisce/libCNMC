from cnmcmodel import CNMCModel


class F8Res4771(CNMCModel):
    """
        Class for eight file of resolution 4771(CT)
    """

    fields = [
        'identificador',
        'cini',
        'denominacion',
        'codigo_tipo_ct',
        'codigo_ccaa',
        'participacion',
        'fecha_aps'
    ]

    @property
    def ref(self):
        return self.store['identificador'][1:]

    def __cmp__(self, other):
        comp_fields = ['cini', 'fecha_aps']
        if self.diff(other, comp_fields):
            return True
        else:
            return False





