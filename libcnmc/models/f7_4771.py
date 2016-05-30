from cnmcmodel import CNMCModel


class F7Res4771(CNMCModel):
    """
        Class for seventh file of resolution 4771(Fiabilidad)
    """

    fields = [
        'identificador',
        'cini',
        'elemento_act',
        'codigo_tipo_inst',
        'codigo_ccaa',
        'fecha_aps'
    ]

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = ['cini', 'elemento_act']
        if self.diff(other, comp_fields):
            return True
        else:
            return False





