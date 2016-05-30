from cnmcmodel import CNMCModel


class F6Res4771(CNMCModel):
    """
        Class for sixth file of resolution 4771(Despatxos)
    """

    fields = [
        'identificador',
        'cini',
        'denominacion',
        'anio_aps',
        'valor_inversion'
    ]

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        # TODO: Add comparsion fields
        comp_fields = []
        if self.diff(other, comp_fields):
            return True
        else:
            return False





