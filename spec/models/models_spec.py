# coding=utf-8
from decimal import Decimal
from libcnmc.models.f2_4771 import F2Res4771


with description('Creating a model'):
    with description('F2 4772 (BT)'):
        with before.all:
            self.input_data = {
                'capacidad': '312,81',
                'participacion': '40',
                'fecha_aps': '03/01/2006',
                'codigo_ccaa_1': '10',
                'codigo_ccaa_2': '10',
                'cini': 'I20571FB',
                'identificador': 'B3',
                'intensidad_maxima': '430',
                'numero_circuitos': '1',
                'seccion': '240',
                'origen': '7738',
                'destino': '7763',
                'numero_conductores': '1',
                'codigo_tipo_linea': 'TI-021',
                'nivel_tension': '0,4',
                'longitud': '0,154',
                'propiedad': '1'
            }
        with it('must convert floats to float with correspondent precision'):
            data = self.input_data.copy()
            f2 = F2Res4771(**data)
            assert f2.store.longitud == Decimal('0.154')
            data = self.input_data.copy()
            data['longitud'] = 0.1545
            f2_native = F2Res4771(**data)
            assert f2_native.store.longitud == Decimal('0.154')
            assert f2_native == f2
