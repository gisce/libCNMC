from cnmcmodel import CNMCModel


class F5Res4771(CNMCModel):
    """
        Class for fifth file of resolution 4771(Maquines)
    """
    fields = [
        'identificador',
        'cini',
        'denominacion',
        'codigo_tipo_maquina',
        'codigo_ccaa',
        'tension_primario',
        'tension_secundario',
        'participacion',
        'fecha_aps',
        'capacidad'
    ]

    @property
    def ref(self):
        return self.store['identificador'][1:]

    def __cmp__(self, other):
        comp_fields = [
            'cini', 'tension_primario', 'tension_secundario',
            'capacidad', 'denominacion', 'fecha_aps',
        ]
        if self.diff(other, comp_fields):
            return True
        else:
            return False





