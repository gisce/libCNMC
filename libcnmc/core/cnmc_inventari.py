# -*- coding: utf-8 -*-

from libcomxml.core import XmlModel, XmlField


class Linea(XmlModel):
    _sort_order = ('linea', 'identificador', 'cini', 'origen',
                   'destino', 'codigo_tipo_linea', 'codigo_ccaa_1',
                   'codigo_ccaa_2', 'participacion', 'fecha_aps',
                   'fecha_baja', 'numero_circuitos', 'numero_conductores',
                   'longitud', 'seccion', 'capacidad')

    def __init__(self):
        self.linea = XmlField('LINEA')
        self.identificador = XmlField('IDENTIFICADOR') 
        self.cini = XmlField('CINI')
        self.origen = XmlField('ORIGEN')
        self.destino = XmlField('DESTINO')
        self.codigo_tipo_linea = XmlField('CODIGO_TIPO_LINEA')
        self.codigo_ccaa_1 = XmlField('CODIGO_CCAA_1')
        self.codigo_ccaa_2 = XmlField('CODIGO_CCAA_2')
        self.participacion = XmlField('PARTICIPACION')
        self.fecha_aps = XmlField('FECHA_APS')
        self.fecha_baja = XmlField('FECHA_BAJA')
        self.numero_circuitos = XmlField('NUMERO_CIRCUITOS')
        self.numero_conductores = XmlField('NUMERO_CONDUCTORES')
        self.longitud = XmlField('LONGITUD')
        self.seccion = XmlField('SECCION')
        self.capacidad = XmlField('CAPACIDAD')

        super(Linea, self).__init__('LINEA', 'linea', drop_empty=False)


class Subestacion(XmlModel):
    _sort_order = ('identificador', 'cini', 'denominacion',
                   'codigo_tipo_posicion', 'codigo_ccaa', 'participacion',
                   'fecha_aps', 'fecha_baja', 'posiciones')

    def __init__(self):
        self.subestacion = XmlField('SUBESTACION')
        self.identificador = XmlField('IDENTIFICADOR')
        self.cini = XmlField('CINI')
        self.denominacion = XmlField('DENOMINACION')
        self.codigo_tipo_posicion = XmlField('CODIGO_TIPO_POSICION')
        self.codigo_ccaa = XmlField('CODIGO_CCAA')
        self.participacion = XmlField('PARTICIPACION')
        self.fecha_aps = XmlField('FECHA_APS')
        self.fecha_baja = XmlField('FECHA_BAJA')
        self.posiciones = XmlField('POSICIONES')

        super(Subestacion, self).__init__('SUBESTACION', 'subestacion',
                                          drop_empty=False)


class Posicion(XmlModel):
    _sort_order = ('identificador', 'cini', 'denominacion',
                   'codigo_tipo_posicion', 'codigo_ccaa', 'participacion',
                   'fecha_aps', 'fecha_baja')

    def __init__(self):
        self.posicion = XmlField('POSICION') 
        self.identificador = XmlField('IDENTIFICADOR') 
        self.cini = XmlField('CINI')
        self.denominacion = XmlField('DENOMINACION') 
        self.codigo_tipo_posicion = XmlField('CODIGO_TIPO_POSICION')
        self.codigo_ccaa = XmlField('CODIGO_CCAA')
        self.participacion = XmlField('PARTICIPACION')
        self.fecha_aps = XmlField('FECHA_APS')
        self.fecha_baja = XmlField('FECHA_BAJA')

        super(Posicion, self).__init__('POSICION', 'posicion',
                                       drop_empty=False)

class Maquina(XmlModel):
    _sort_order = ('identificador', 'cini', 'denominacion',
                   'codigo_tipo_maquina', 'codigo_zona', 'codigo_ccaa',
                   'participacion', 'fecha_aps', 'fecha_baja', 'capacidad')

    def __init__(self):
        self.maquina = XmlField('MAQUINA')
        self.identificador = XmlField('IDENTIFICADOR') 
        self.cini = XmlField('CINI')
        self.denominacion = XmlField('DENOMINACION') 
        self.codigo_tipo_maquina = XmlField('CODIGO_TIPO_MAQUINA')
        self.codigo_zona = XmlField('CODIGO_ZONA')
        self.codigo_ccaa = XmlField('CODIGO_CCAA')
        self.participacion = XmlField('PARTICIPACION')
        self.fecha_aps = XmlField('FECHA_APS')
        self.fecha_baja = XmlField('FECHA_BAJA')
        self.capacidad = XmlField('CAPACIDAD')
        
        super(Maquina, self).__init__('MAQUINA', 'maquina', drop_empty=False)


class Despacho(XmlModel):
    _sort_order = ('identificador', 'cini', 'denominacion', 'anyo_ps', 'vai')

    def __init__(self):
        self.despacho = XmlField('DESPACHO')
        self.identificador = XmlField('IDENTIFICADOR') 
        self.cini = XmlField('CINI')
        self.denominacion = XmlField('DENOMINACION') 
        self.anyo_ps = XmlField(u'AÑO_PS')
        self.vai = XmlField('VAI')

        super(Despacho, self).__init__('DESPACHO', 'despacho', drop_empty=False)


class Fiabilidad(XmlModel):
    _sort_order = ('identificador', 'cini', 'denominacion',
                   'codigo_tipo_inst', 'codigo_ccaa', 'fecha_aps', 'fecha_baja')

    def __init__(self):
        self.fiabilidad = XmlField('FIABILIDAD')
        self.identificador = XmlField('IDENTIFICADOR') 
        self.cini = XmlField('CINI')
        self.denominacion = XmlField('DENOMINACION') 
        self.codigo_tipo_inst = XmlField('CODIGO_TIPO_INST')
        self.codigo_ccaa = XmlField('CODIGO_CCAA')
        self.fecha_aps = XmlField('FECHA_APS')
        self.fecha_baja = XmlField('FECHA_BAJA')

        super(Fiabilidad, self).__init__('FIABILIDAD', 'fiabilidad',
                                         drop_empty=False)


class Transformacion(XmlModel):
    _sort_order = ('identificador', 'cini', 'denominacion', 'codigo_tipo_ct',
                   'codigo_ccaa', 'participacion', 'fecha_aps', 'fecha_baja')

    def __init__(self):
        self.transformacion = XmlField('TRANSFORMACION')
        self.identificador = XmlField('IDENTIFICADOR')
        self.cini = XmlField('CINI')
        self.denominacion = XmlField('DENOMINACION')
        self.codigo_tipo_ct = XmlField('CODIGO_TIPO_CT')
        self.codigo_ccaa = XmlField('CODIGO_CCAA')
        self.participacion = XmlField('PARTICIPACION')
        self.fecha_aps = XmlField('FECHA_APS')
        self.fecha_baja = XmlField('FECHA_BAJA')

        super(Transformacion, self).__init__('TRANSFORMACION', 'transformacion',
                                             drop_empty=False)



class Empresa(XmlModel):
    _sort_order = ('root', 'linea', 'posicion', 'maquina', 'despacho',
                   'fiabilidad', 'transformacion')

    def __init__(self, codigo=''):
        self.root = XmlField('EMPRESA', attributes={'CODIGO': codigo})
        self.linea = []
        self.subestacion = []
        self.posicion = []
        self.maquina = []
        self.despacho = []
        self.fiabilidad = []
        self.transformacion = []
        
        super(Empresa, self).__init__('EMPRESA', 'root', drop_empty=False)

    def set_codigo(self, codigo):
        self.root.attributes.update({'CODIGO': codigo})
