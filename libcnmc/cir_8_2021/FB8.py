#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback, psycopg2.extras
from libcnmc.utils import format_f, convert_srid, get_srid
from libcnmc.core import MultiprocessBased
from libcnmc.utils import (
    format_f, get_id_municipi_from_company, get_forced_elements, adapt_diff, convert_srid, get_srid, format_f,
    convert_spanish_date, get_name_ti, format_f_6181, get_codi_actuacio, get_ine
)


class FB8(MultiprocessBased):

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
            'valor_baixa_parcial', 'motivacion'
        ]
        fields_to_read_obra = [
            'subvenciones_europeas', 'subvenciones_nacionales', 'subvenciones_prtr', 'financiado', 'fecha_baja',
            'cuenta_contable', 'im_ingenieria', 'im_materiales', 'im_obracivil', 'im_trabajos'
        ]

        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                despatx = O.GiscedataDespatx.read(
                    item, fields_to_read
                )

                # OBRES

                obra_id = O.GiscedataProjecteObraTiDespatx.search([('element_ti_id', '=', despatx['id'])])

                #DATA_PM
                if despatx['data_apm']:
                    data_pm_despatx = datetime.strptime(str(despatx['data_apm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_despatx.strftime('%d/%m/%Y')

                # OBRES

                obra_ti_despatx_obj = O.GiscedataProjecteObraTiDespatx
                obra_ti_despatx_id = obra_ti_despatx_obj.search([('element_ti_id', '=', despatx['id'])])
                if obra_ti_despatx_id:
                    obra_id_data = obra_ti_despatx_obj.read(obra_ti_despatx_id[0], ['obra_id'])
                else:
                    obra_id_data = {}

                # Filtre d'obres finalitzades
                despatx_obra = ''
                if obra_id_data.get('obra_id', False):
                    obra_id = obra_id_data['obra_id']
                    data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                    if data_finalitzacio_data:
                        if data_finalitzacio_data.get('data_finalitzacio', False):
                            data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                            inici_any = '{}-01-01'.format(self.year)
                            fi_any = '{}-12-31'.format(self.year)
                            if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                despatx_obra = O.GiscedataProjecteObraTiDespatx.read(obra_ti_despatx_id[0],
                                                                                     fields_to_read_obra)
                else:
                    despatx_obra = ''

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
                    motivacion = get_codi_actuacio(O, despatx['motivacion'] and despatx['motivacion'][0])
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

                if id_municipi:
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat_codi = comunitat_vals['codi']

                # TODO: Temporal
                estat = 0

                descripcio = ''

                self.output_q.put([
                    despatx['id'],                      # IDENTIFICADOR
                    despatx['cini'],                    # CINI
                    motivacion,                         # MOTIVACION
                    estat,                              # ESTADO
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
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()