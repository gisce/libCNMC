#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f_6181, get_name_ti, get_codi_actuacio, \
    format_ccaa_code, convert_spanish_date, format_f, adapt_diff, get_forced_elements
from libcnmc.models import F4Res4666

INTERRUPTOR = {
    '1': '0', #PARQUE
    '2': '1', #INTERRUPTOR AUTOMÁTICO
    '3': '2'  #SIN INTERRUPTOR
}

MODELO = {
    '1': 'I',
    '2': 'M',
    '3': 'D',
    '4': 'E'
}
class FB4(StopMultiprocessBased):

    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        """
            Class constructor
            :param kwargs:
            """

        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.compare_field = '4666_entregada'

        super(FB4, self).__init__(**kwargs)
        self.include_obres = False
        if kwargs.get("include_obra", False):
            self.include_obres = True
        if kwargs.get("include_header", False):
            self.file_header = self.get_header()
        self.all_int = kwargs.get('all_int', True)


    def get_header(self):
        header = [
            'IDENTIFICADOR', 'CINI', 'TIPO_INVERSION', 'DENOMINACION', 'CODIGO_CCUU', 'CODIGO_CCAA',
            'IDENTIFICADOR_PARQUE', 'NIVEL_TENSION_EXPLOTACION', 'FINANCIADO', 'PLANIFICACION', 'FECHA_APS',
            'FECHA_BAJA', 'CAUSA_BAJA', 'IM_INGENIERIA', 'IM_MATERIALES', 'IM_OBRACIVIL', 'IM_TRABAJOS',
            'SUBVENCIONES_EUROPEAS', 'SUBVENCIONES_NACIONALES', 'VALOR_AUDITADO', 'VALOR_CONTABLE', 'CUENTA_CONTABLE',
            'PORCENTAJE_MODIFICACION', 'MOTIVACION', 'IDENTIFICADOR_BAJA',
        ]
        if self.include_obres:
            header.insert(0, 'IDENTIFICADOR_OBRA')
        return header

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """
        if self.all_int:
            search_params = [
                ("interruptor", "in", ('2', '3'))
            ]
        else:
            search_params = [
                ("interruptor", "=", '2')
            ]

        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params += ['|', ('data_pm', '=', False),
                          ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                          ('data_baixa', '=', False),
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                          ('data_baixa', '!=', False),
                          ('active', '=', True)]

        forced_ids = get_forced_elements(self.connection, "giscedata.cts.subestacions.posicio")

        ids = self.connection.GiscedataCtsSubestacionsPosicio.search(
            search_params, 0, 0, False, {'active_test': False})

        ids = ids + forced_ids["include"]
        ids = list(set(ids) - set(forced_ids["exclude"]))

        return list(set(ids))

    def get_cts_data(self, sub_id):
        o = self.connection
        cts_id = o.GiscedataCtsSubestacions.read(sub_id, ['ct_id'])
        if cts_id:
            cts_data = o.GiscedataCts.read(cts_id['ct_id'][0], ['propietari', 'node_id', 'punt_frontera', 'model'])
        return cts_data

    def consumer(self):
        """
        Generates the line of the file
        :return: Line
        :rtype: str
        """
        O = self.connection

        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)

        fields_to_read = [
            'name', 'cini', 'node_id', 'propietari', 'subestacio_id', 'data_pm', 'tensio', 'model',
            'parc_id', 'data_baixa', 'interruptor', 'tipus_instalacio_cnmc_id', 'punt_frontera', self.compare_field,
            'tipus_interruptor',
        ]
        fields_to_read_obra = [
            'name', 'cini', 'tipo_inversion', 'denominacion', 'ccuu', 'codigo_ccaa', 'identificador_parque',
            'nivel_tension_explotacion', 'financiado','planificacion','fecha_aps','fecha_baja','causa_baja',
            'im_ingenieria','im_materiales','im_obracivil','im_trabajos','subvenciones_europeas',
            'subvenciones_nacionales','subvenciones_prtr','valor_auditado','valor_residual','valor_contabilidad','cuenta_contable',
            'porcentaje_modificacion','motivacion','obra_id','identificador_baja',
        ]

        def get_inst_name(element_id):
            vals = self.connection.GiscedataCtsSubestacionsPosicio.read(
                element_id, ['name'])
            return vals['name']

        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                pos = O.GiscedataCtsSubestacionsPosicio.read(
                    item, fields_to_read
                )

                # MODEL
                if pos['model']:
                    modelo = pos['model']
                else:
                    modelo = ''

                # Fecha APS
                data_pm = ''
                if pos['data_pm']:
                    data_pm_ct = datetime.strptime(str(pos['data_pm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_ct.strftime('%d/%m/%Y')

                # FECHA_BAJA, CAUSA_BAJA
                if pos['data_baixa']:
                    if pos['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            pos['data_baixa'], '%Y-%m-%d %H:%M:%S')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')

                        if int(fecha_baja.split("/")[2]) - int(data_pm.split("/")[2]) >= 40:
                            if identificador_baja != '':
                                causa_baja = 1
                            else:
                                causa_baja = 2
                        else:
                            causa_baja = 3
                    else:
                        fecha_baja = ''
                        causa_baja = 0;
                else:
                    fecha_baja = ''
                    causa_baja = 0;

                # OBRES
                pos_obra = ''
                obra_ti_pos_obj = O.GiscedataProjecteObraTiPosicio
                obra_ti_ids = obra_ti_pos_obj.search([('element_ti_id', '=', pos['id'])])
                if obra_ti_ids:
                    for obra_ti_id in obra_ti_ids:
                        obra_id_data = obra_ti_pos_obj.read(obra_ti_id, ['obra_id'])
                        obra_id = obra_id_data['obra_id']
                        # Filtre d'obres finalitzades
                        data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                        if data_finalitzacio_data:
                            if data_finalitzacio_data.get('data_finalitzacio', False):
                                data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                                inici_any = '{}-01-01'.format(self.year)
                                fi_any = '{}-12-31'.format(self.year)
                                if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                    pos_obra = O.GiscedataProjecteObraTiPosicio.read(obra_ti_id, fields_to_read_obra)
                        if pos_obra:
                            break

                tipo_inversion = ''
                #CAMPS OBRES
                if pos_obra != '':
                    obra_year = data_finalitzacio.split('-')[0]
                    data_pm_year = data_pm.split('/')[2]
                    if pos_obra['tipo_inversion'] != '0' and obra_year != data_pm_year:
                        data_ip = convert_spanish_date(data_finalitzacio)
                    else:
                        data_ip = ''
                    im_materiales = format_f_6181(pos_obra['im_materiales'] or 0.0, float_type='euro')
                    im_obracivil = format_f_6181(pos_obra['im_obracivil'] or 0.0, float_type='euro')
                    im_construccion = str(format_f(
                        float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", "."))
                    , 2)).replace(".", ",")
                    im_ingenieria = format_f_6181(pos_obra['im_ingenieria'] or 0.0, float_type='euro')
                    im_trabajos = format_f_6181(pos_obra['im_trabajos'] or 0.0, float_type='euro')
                    tipo_inversion = pos_obra['tipo_inversion'] or ''
                    valor_auditado = format_f_6181(pos_obra['valor_auditado'] or 0.0, float_type='euro')
                    valor_residual = format_f_6181(pos_obra['valor_residual'] or 0.0, float_type='euro')
                    subvenciones_europeas = format_f_6181(pos_obra['subvenciones_europeas'] or 0.0, float_type='euro')
                    subvenciones_nacionales = format_f_6181(pos_obra['subvenciones_nacionales'] or 0.0, float_type='euro')
                    subvenciones_prtr = format_f_6181(pos_obra['subvenciones_prtr'] or 0.0, float_type='euro')
                    cuenta_contable = pos_obra['cuenta_contable']
                    financiado = format_f(pos_obra.get('financiado', 0.0), 2)
                    motivacion = get_codi_actuacio(O, pos_obra['motivacion'] and pos_obra['motivacion'][0])

                    identificador_baja = (
                        get_inst_name(pos_obra['identificador_baja'][0])  # IDENTIFICADOR_BAJA
                        if pos_obra['identificador_baja'] else ''
                    )
                else:
                    data_ip = ''
                    identificador_baja = ''
                    im_ingenieria = ''
                    im_construccion = ''
                    im_trabajos = ''
                    subvenciones_europeas = ''
                    subvenciones_nacionales = ''
                    subvenciones_prtr = ''
                    valor_auditado = ''
                    valor_residual = ''
                    motivacion = ''
                    cuenta_contable = ''
                    financiado = ''

                # Si la data APS es igual a l'any de la generació del fitxer,
                # la data IP sortirà en blanc
                if data_ip:
                    data_ip = '' if data_pm and int(data_pm.split('/')[2]) == int(data_ip.split('/')[2]) \
                    else data_ip

                #IDENTIFICADOR_EMPLAZAMIENTO
                if pos['parc_id']:
                    identificador_emplazamiento = pos['parc_id'][1]
                else:
                    o_parc = pos['subestacio_id'][1] + "-"\
                        + str(self.get_tensio(pos))
                    identificador_emplazamiento = "SUBESTACIO_NAME"

                #CODIGO CCUU
                if pos['tipus_instalacio_cnmc_id']:
                    id_ti = pos['tipus_instalacio_cnmc_id'][0]
                    ti = O.GiscedataTipusInstallacio.read(
                        id_ti,
                        ['name'])['name']
                else:
                    ti = ''

                #AJENA
                if pos['propietari']:
                    ajena = 0
                else:
                    ajena = 1

                #NODE
                if pos['node_id']:
                    node = pos['node_id'][1]
                else:
                    node = ''

                #PUNT_FRONTERA
                punt_frontera = int(pos['punt_frontera'] == True)

                #EQUIPADA
                equipada = ''
                if pos.get('tipus_interruptor', False):
                    interruptor = pos['tipus_interruptor']
                    if interruptor == 'P':
                        equipada = '0'
                    else:
                        equipada = interruptor

                if pos[self.compare_field]:
                    last_data = pos[self.compare_field]
                    entregada = F4Res4666(**last_data)
                    actual = F4Res4666(
                        pos['name'],
                        pos['cini'],
                        '',
                        str(ti),
                        '',
                        '',
                        '',
                        data_pm,
                        fecha_baja,
                        0
                    )
                    if entregada == actual and fecha_baja == '':
                        estado = '0'
                        if pos_obra:
                            estado = '1'
                    else:
                        self.output_m.put("{} {}".format(pos["name"], adapt_diff(actual.diff(entregada))))
                        estado = '1'
                else:
                    estado = '2'

                if pos['cini'][4] == '3' and data_pm < data_baixa_limit and pos_obra == '':
                    estado = '0'

                # Si MODELO = 'M', ESTADO i FECHA_APS han d'estar buides
                if modelo == 'M':
                    estado = ''
                    data_pm = ''

                if fecha_baja:
                    tipo_inversion = ''

                # L'any 2022 no es declaren subvencions PRTR
                subvenciones_prtr = ''

                if causa_baja == '0':
                    fecha_baja = ''

                if modelo == 'E':
                    tipo_inversion = '0'
                    estado = '1'

                # ESTADO no pot ser 2 si FECHA_APS < 2022
                fecha_aps_year = int(data_pm.split('/')[2])
                if estado == '2' and fecha_aps_year != int(self.year):
                    estado = '1'
                elif fecha_aps_year == int(self.year):
                    estado = '2'

                output = [
                    pos['name'],  #IDENTIFICADOR_POSICION
                    pos['cini'],  #CINI
                    node,    #NUDO
                    str(ti),      #CODIGO_CCUU
                    identificador_emplazamiento, #IDENTIFICADOR EMPLAZAMIENTO
                    ajena,  #AJENA
                    equipada,   #EQUIPADA
                    estado,     #ESTADO
                    modelo,         #MODELO
                    punt_frontera,  #PUNTO_FRONTERA
                    data_pm,      #FECHA_APS
                    fecha_baja,     #FECHA_BAJA
                    causa_baja,     #CAUSA_BAJA
                    data_ip,    #fecha IP
                    tipo_inversion,  # TIPO_INVERSION
                    im_ingenieria,    #IM_TRAMITES
                    im_construccion,    #IM_CONSTRUCCION
                    im_trabajos,      #IM_TRABAJOS
                    valor_auditado,   #VALOR_AUDITADO
                    valor_residual,     #VALOR RESIDUAL
                    subvenciones_europeas,    #SUBVENCIONES EUROPEAS
                    subvenciones_nacionales,  #SUBVECIONES NACIONALES
                    subvenciones_prtr,  #SUBVENCIONES_PRTR
                    cuenta_contable,   #CUENTA
                    financiado,   #FINANCIADO
                    motivacion,    #MOTIVACION
                    identificador_baja, #IDENTIFICADOR BAJA
                ]
                if self.include_obres:
                    output.insert(0, pos_obra['obra_id'][1])
                output = map(lambda e: '' if e is False or e is None else e, output)
                self.output_q.put(output)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
