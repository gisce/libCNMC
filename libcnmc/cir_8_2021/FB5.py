#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback, psycopg2.extras
from libcnmc.utils import format_f, convert_srid, get_srid, get_norm_tension
from libcnmc.core import MultiprocessBased
from libcnmc.utils import (
    format_f, get_id_municipi_from_company, get_forced_elements, adapt_diff, convert_srid, get_srid, format_f,
    convert_spanish_date, get_name_ti, format_f_6181, get_codi_actuacio, get_ine
)

class FB5(MultiprocessBased):
    def __init__(self, **kwargs):
        super(FB5, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB5 - TRAFOS-SE'
        self.base_object = 'TRAFOS'

    def get_sequence(self):
        search_params = [
            ('reductor', '=', True),
            ('id_estat.cnmc_inventari', '=', True)
        ]
        return self.connection.GiscedataTransformadorTrafo.search(
            search_params
        )

    def get_estat(self, estat_id):
        o = self.connection
        estat = o.GiscedataTransformadorEstat.read(estat_id, ['codi'])
        if estat['codi'] != 1:
            return 0
        else:
            return 1

    def get_costat_alta(self, trafo):
        o = self.connection
        res = ''
        if trafo['conexions']:
            con = o.GiscedataTransformadorConnexio.read(trafo['conexions'][0])
            tensio = con['tensio_primari']
            tensio_n = get_norm_tension(o, tensio)
            se_id = trafo['ct'][1]
            parc_id = o.GiscedataParcs.search(
                [
                    ('subestacio_id', '=', se_id),
                    ('tensio_id.tensio', '=', tensio_n)
                ]
            )
            if parc_id:
                res = o.GiscedataParcs.read(parc_id[0], ['subestacio_id'])['subestacio_id']

        return res

    def get_costat_baixa(self, trafo):
        o = self.connection
        res = ''
        if trafo['conexions']:
            con = o.GiscedataTransformadorConnexio.read(trafo['conexions'][0])
            tensio = con['tensio_b1']
            tensio_n = get_norm_tension(o, tensio)
            se_id = trafo['ct'][1]
            parc_id = o.GiscedataParcs.search(
                [
                    ('subestacio_id', '=', se_id),
                    ('tensio_id.tensio', '=', tensio_n)
                ]
            )
            if parc_id:
                res = o.GiscedataParcs.read(parc_id[0], ['name'])['name']
        return res


    def consumer(self):
        o = self.connection
        fields_to_read = [
            'ct', 'name', 'cini', 'potencia_nominal', 'id_estat',
            'conexions', 'data_pm', 'data_baixa'
        ]

        fields_to_read_obra = [
            'subvenciones_europeas', 'subvenciones_nacionales', 'subvenciones_prtr', 'financiado',
            'fecha_aps', 'fecha_baja', 'causa_baja', 'cuenta_contable', 'im_ingenieria', 'im_materiales',
            'im_obracivil', 'im_trabajos', 'motivacion', 'tipo_inversion', 'valor_residual'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                trafo = o.GiscedataTransformadorTrafo.read(
                    item, fields_to_read
                )

                obra_id = o.GiscedataProjecteObraTiTransformador.search([('element_ti_id', '=', trafo['id'])])

                if obra_id:
                    linia = o.GiscedataProjecteObraTiTransformador.read(obra_id, fields_to_read_obra)[0]
                else:
                    linia = ''

                if linia != '':
                    data_ip = convert_spanish_date(
                            linia['fecha_aps'] if not linia['fecha_baja'] and linia['tipo_inversion'] != '1' else ''
                    )
                    subvenciones_europeas = format_f_6181(linia['subvenciones_europeas'] or 0.0, float_type='euro')
                    subvenciones_nacionales = format_f_6181(linia['subvenciones_nacionales'] or 0.0, float_type='euro')
                    subvenciones_prtr = format_f_6181(linia['subvenciones_prtr'] or 0.0, float_type='euro')
                    im_ingenieria = format_f_6181(linia['im_ingenieria'] or 0.0, float_type='euro')
                    im_materiales = format_f_6181(linia['im_materiales'] or 0.0, float_type='euro')
                    im_obracivil = format_f_6181(linia['im_obracivil'] or 0.0, float_type='euro')
                    im_trabajos = format_f_6181(linia['im_trabajos'] or 0.0, float_type='euro')
                    im_construccion = str(
                        float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", "."))
                    ).replace(".", ",")
                    tipo_inversion = (linia['tipo_inversion'] or '0') if not linia['fecha_baja'] else '1'
                    valor_auditado = str(
                        float(im_construccion.replace(",", ".")) +
                        float(im_ingenieria.replace(",", ".")) + float(im_trabajos.replace(",", "."))
                    ).replace(".", ",")

                    valor_residual = linia['valor_residual']

                    cuenta_contable = linia['cuenta_contable']
                    financiado = format_f(
                        100.0 - linia.get('financiado', 0.0), 2
                    )
                    motivacion = linia['motivacion']
                else:
                    data_ip = ''
                    subvenciones_europeas = ''
                    subvenciones_nacionales = ''
                    subvenciones_prtr = ''
                    valor_auditado = ''
                    cuenta_contable = ''
                    financiado = ''
                    motivacion = ''
                    valor_residual = ''

                # Si la data APS es igual a l'any de la generació del fitxer,
                # la data APS sortirà en blanc
                if data_ip:
                    data_ip = '' if data_pm and int(data_pm.split('/')[2]) == int(data_ip.split('/')[2]) \
                        else data_ip

                o_subestacio = trafo['ct'][1]
                o_maquina = trafo['name']
                o_cini = trafo['cini']
                o_costat_alta = self.get_costat_alta(trafo)
                o_costat_baixa = self.get_costat_baixa(trafo)
                o_pot_maquina = format_f(
                    float(trafo['potencia_nominal']) / 1000.0, decimals=3)

                if trafo['data_apm']:
                    data_pm_trafo = datetime.strptime(str(trafo['data_apm']),
                                                        '%Y-%m-%d')
                    data_pm = data_pm_trafo.strftime('%d/%m/%Y')

                if trafo['data_baixa']:
                    if trafo['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            trafo['data_baixa'], '%Y-%m-%d')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')

                        if int(data_pm.split("/")[2]) - int(fecha_baja.split("/")[2]) >= 40:
                            if identificador_baja != '':
                                causa_baja = 1
                            else:
                                causa_baja = 2
                    else:
                        fecha_baja = ''
                        causa_baja = 0;
                else:
                    fecha_baja = ''
                    causa_baja = 0;

                o_estat = self.get_estat(trafo['id_estat'][0])

                id_ti = ct['tipus_instalacio_cnmc_id'][0]
                ti = o.GiscedataTipusInstallacio.read(
                    id_ti,
                    ['name'])['name']

                self.output_q.put([
                    o_maquina,              # IDENTIFICADOR_MAQUINA
                    o_cini,                 # CINI
                    ti,                     # CCUU
                    #NUDO_ALTA
                    #NUDO_BAJA
                    o_pot_maquina,  # POTENCIA MAQUINA
                    #o_estat,  # ESTADO
                    #modelo,     #MODELO
                    data_pm,               #FECHA_APS
                    fecha_baja,            #FECHA_BAJA
                    causa_baja,            #CAUSA_BAJA
                    data_ip,            #FECHA_IP
                    tipo_inversion,     #TIPO_INVERSION
                    im_ingenieria,      #IM_TRAMITES
                    im_construccion,    #IM_CONSTRUCCION
                    im_trabajos,        #IM_TRABAJOS
                    valor_residual,     #VALOR_RESIDUAL
                    subvenciones_europeas,  #SUBVENCIONES_EUROPEAS
                    subvenciones_nacionales, #SUBVENCIONES_NACIONALES
                    subvenciones_prtr,  #SUBVENCIONES_PRTR
                    valor_auditado,          #VALOR_AUDITADO
                    financiado,         #FINANCIADO
                    cuenta_contable,       #CUENTA
                    motivacion,         #MOTIVACION
                    identificador_baja,     #IDENTIFICADOR BAJA
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()