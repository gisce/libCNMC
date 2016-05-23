from cnmcmodel import CNMCModel


class F3Res4771(CNMCModel):
    """
        Model for third file of 4771 resolution(Subestacions)
    """
    fields = [
        'identificador',
        'cini',
        'denominacion',
        'participacion',
        'fecha_aps',
        'posiciones',
    ]

    @property
    def ref(self):
        return self.store['identificador']

    def __cmp__(self, other):
        comp_fields = ['fecha_aps']
        if self.diff(other, comp_fields):
            return True
        else:
            return False





