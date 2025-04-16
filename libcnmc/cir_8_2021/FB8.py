#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback, psycopg2.extras
from libcnmc.utils import format_f, convert_srid, get_srid
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import (
    format_f, get_id_municipi_from_company, get_forced_elements, adapt_diff, convert_srid, get_srid, format_f,
    convert_spanish_date, get_name_ti, format_f_6181, get_codi_actuacio, get_ine
)
from libcnmc.models import F6Res4666


class FB8(StopMultiprocessBased):

    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        super(FB8, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB8 - Otros activos'
        self.base_object = 'Despatxos'

    def get_sequence(self):
        data_pm = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)

        search_params = ['|', ('data_apm', '=', False),
                          ('data_apm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                          ('data_baixa', '=', False),
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataDespatx.search(search_params, 0, 0, False, {'active_test': False}
        )

    def consumer(self):
        O = self.connection
        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)
        fields_to_read = [
            'id', 'cini', 'name', 'geom', 'vertex', 'data_apm', 'data_baixa', 'municipi', 'data_baixa_parcial',
            'valor_baixa_parcial', 'motivacion', 'coco',
        ]
        fields_to_read_obra = [
            'subvenciones_europeas', 'subvenciones_nacionales', 'subvenciones_prtr', 'financiado', 'fecha_baja',
            'cuenta_contable', 'im_ingenieria', 'im_materiales', 'im_obracivil', 'im_trabajos'
        ]

        while True:
            try:
                # generar linies
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                despatx = O.GiscedataDespatx.read(
                    item, fields_to_read
                )

                # OBRES

                obra_id = O.GiscedataProjecteObraTiDespatx.search([('element_ti_id', '=', despatx['id'])])

                #DATA_PM
                data_pm = ''
                if despatx['data_apm']:
                    data_pm_despatx = datetime.strptime(str(despatx['data_apm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_despatx.strftime('%d/%m/%Y')

                # OBRES
                despatx_obra = ''
                obra_ti_despatx_obj = O.GiscedataProjecteObraTiDespatx
                obra_ti_ids = obra_ti_despatx_obj.search([('element_ti_id', '=', despatx['id'])])
                if obra_ti_ids:
                    for obra_ti_id in obra_ti_ids:
                        obra_id_data = obra_ti_despatx_obj.read(obra_ti_id, ['obra_id'])
                        obra_id = obra_id_data['obra_id']
                        # Filtre d'obres finalitzades
                        data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                        if data_finalitzacio_data:
                            if data_finalitzacio_data.get('data_finalitzacio', False):
                                data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                                inici_any = '{}-01-01'.format(self.year)
                                fi_any = '{}-12-31'.format(self.year)
                                if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                    despatx_obra = O.GiscedataProjecteObraTiDespatx.read(obra_ti_id,
                                                                                         fields_to_read_obra)
                        if despatx_obra:
                            break

                #CAMPS OBRES
                if despatx_obra != '':
                    subvenciones_europeas = format_f_6181(despatx_obra['subvenciones_europeas'] or 0.0, float_type='euro')
                    subvenciones_nacionales = format_f_6181(despatx_obra['subvenciones_nacionales'] or 0.0, float_type='euro')
                    subvenciones_prtr = format_f_6181(despatx_obra['subvenciones_prtr'] or 0.0, float_type='euro')
                    im_ingenieria = format_f_6181(despatx_obra['im_ingenieria'] or 0.0, float_type='euro')
                    im_materiales = format_f_6181(despatx_obra['im_materiales'] or 0.0, float_type='euro')
                    im_obracivil = format_f_6181(despatx_obra['im_obracivil'] or 0.0, float_type='euro')
                    im_trabajos = format_f_6181(despatx_obra['im_trabajos'] or 0.0, float_type='euro')
                    valor_auditado = str(
                        float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", ".")) +
                        float(im_ingenieria.replace(",", ".")) + float(im_trabajos.replace(",", "."))
                    ).replace(".", ",")

                    cuenta_contable = despatx_obra['cuenta_contable']
                    financiado = format_f(despatx_obra.get('financiado', 0.0), 2)
                else:
                    subvenciones_europeas = ''
                    subvenciones_nacionales = ''
                    subvenciones_prtr = ''
                    valor_auditado = ''
                    cuenta_contable = ''
                    financiado = ''

                if despatx['motivacion']:
                    motivacion = despatx['motivacion']
                else:
                    motivacion = ''

                data_baixa_parcial = ''
                valor_baixa_parcial = ''
                if despatx['data_baixa_parcial']:
                    data_baixa_parcial = datetime.strptime(str(despatx['data_pm']),
                                                   '%Y-%m-%d')
                    data_baixa_parcial = data_pm_despatx.strftime('%d/%m/%Y')
                    valor_baixa_parcial = despatx['valor_baixa_parcial']
                    causa_baja = 4
                elif despatx['data_baixa']:
                    if despatx['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            despatx['data_baixa'], '%Y-%m-%d')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')

                        if int(fecha_baja.split("/")[2]) - int(data_pm.split("/")[2]) >= 40:
                            causa_baja = 2
                        else:
                            causa_baja = 3
                    else:
                        fecha_baja = ''
                        causa_baja = 0;
                else:
                    fecha_baja = ''
                    causa_baja = 0;

                #funció per trobar la ccaa desde el municipi
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                if despatx['municipi']:
                    id_municipi = despatx['municipi'][0]
                else:
                    id_municipi = get_id_municipi_from_company(O)

                comunitat_codi = ''
                if id_municipi:
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat_codi = comunitat_vals['codi']

                hist_obj = O.model('circular.82021.historics.b8')
                hist_ids = hist_obj.search([
                    ('identificador', '=', despatx['name']),
                    ('year', '=', self.year - 1)
                ])
                if hist_ids:
                    hist = hist_obj.read(hist_ids[0], [
                        'cini', 'codigo_ccuu', 'fecha_aps'
                    ])
                    entregada = F6Res4666(
                        cini=hist['cini'],
                        fecha_aps=hist['fecha_aps']
                    )
                    actual = F6Res4666(
                        despatx['name'],
                        despatx['cini'],
                        '',
                        data_pm,
                        fecha_baja,
                        '',
                        0
                    )
                    if actual == entregada:
                        estado = '0'
                        if despatx_obra:
                            estado = '1'
                    else:
                        self.output_m.put("{} {}".format(despatx["name"], adapt_diff(actual.diff(entregada))))
                        self.output_m.put("Identificador:{} diff:{}".format(despatx["name"], actual.diff(entregada)))
                        estado = '1'
                else:
                    estado = '2'
                    if data_pm and int(data_pm.split('/')[2]) != int(self.year):
                        estado = '1'

                if despatx.get('coco', False):
                    descripcio = despatx['coco']
                else:
                    descripcio = ''

                # L'any 2022 no es declaren subvencions PRTR
                subvenciones_prtr = ''

                self.output_q.put([
                    despatx['name'],                    # IDENTIFICADOR
                    despatx['cini'],                    # CINI
                    motivacion,                         # MOTIVACION
                    estado,                             # ESTADO
                    descripcio,                         # DESCRIPCION
                    comunitat_codi,                     # CCAA
                    data_pm,                            # FECHA_APS
                    causa_baja,                         # CAUSA_BAJA
                    fecha_baja,                         # FECHA_BAJA
                    data_baixa_parcial,                 # FECHA_BAJA_PARCIAL
                    valor_baixa_parcial,                # VALOR_BAJA_PARCIAL
                    subvenciones_europeas,              # SUBVENCIONES_EUROPEAS
                    subvenciones_nacionales,            # SUBVENCIONES_NACIONALES
                    subvenciones_prtr,                  # SUBVENCIONES_PRTR
                    valor_auditado,                     # VALOR_AUDITADO
                    financiado,                         # FINANCIADO
                    cuenta_contable,                    # CUENTA
                ])
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
