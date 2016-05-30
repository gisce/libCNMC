from cnmcmodel import CNMCModel


class F4Res4771(CNMCModel):
    """
        Class for forth file of resolution 4771(Posiciones)
    """
    fields = [
        'identificador',
        'cini',
        'denominacion',
        'codigo_tipo_posicion',
        'codigo_ccaa',
        'nivel_tension',
        'participacion',
        'fecha_aps',
    ]

    @property
    def ref(self):
        return self.store.identificador

    def __cmp__(self, other):
        comp_fields = [
            'cini', 'nivel_tension', 'participacion',
            'denominacion', 'fecha_aps'
        ]
        if self.diff(other, comp_fields):
            return True
        else:
            return False





