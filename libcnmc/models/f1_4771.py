from cnmcmodel import CNMCModel


class F1Res4771(CNMCModel):
    """
        Class for second file of resolution 4771(LAT)
    """

    fields = [
        'identificador',
        'cini',
        'origen',
        'destino',
        'codigo_tipo_linea',
        'codigo_ccaa_1',
        'codigo_ccaa_2',
        'participacion',
        'fecha_aps',
        'numero_circuitos',
        'numero_conductores',
        'nivel_tension',
        'longitud',
        'intensidad_maxima',
        'seccion',
        'capacidad',
        'propiedad'
        ]

    @property
    def ref(self):
        return self.store.identificador[1:]

    def __cmp__(self, other):
        comp_fields = [
            'longitud', 'cini', 'seccion', 'capacidad', 'fecha_aps'
        ]
        if self.diff(other, comp_fields):
            return True
        else:
            return False





