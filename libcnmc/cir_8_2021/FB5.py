#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback, psycopg2.extras
from libcnmc.utils import format_f, convert_srid, get_srid, get_norm_tension
from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import (
    format_f, get_id_municipi_from_company, get_forced_elements, adapt_diff, convert_srid, get_srid, format_f,
    convert_spanish_date, get_name_ti, format_f_6181, get_codi_actuacio, get_ine, default_estado, calculate_estado
)
from libcnmc.models import F5Res4666
from shapely import wkt


class FB5(StopMultiprocessBased):
    def __init__(self, **kwargs):
        super(FB5, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'FB5 - TRAFOS-SE'
        self.base_object = 'TRAFOS'

    def get_sequence(self):
        search_params = [
            ('criteri_regulatori', '!=', 'excloure'),
            ('reductor', '=', True),
            ('id_estat.cnmc_inventari', '=', True),
        ]
        trafo_ids = [
            'T.{}'.format(x)
            for x in self.connection.GiscedataTransformadorTrafo.search(
                search_params
            )
        ]

        # Condensadors
        search_params = [
            ('criteri_regulatori', '!=', 'excloure'),
            ('ct_id.id_installacio.name', '=', 'SE'),
            ('tipus', '=', '2'),
        ]

        condensadors_ids = [
            'C.{}'.format(x) for x in
            self.connection.GiscedataCondensadors.search(search_params)
        ]
        return trafo_ids + condensadors_ids

    def get_node_vertex(self, ct_id):
        O = self.connection
        bloc = O.GiscegisBlocsCtat.search([('ct', '=', ct_id)])
        node = ''
        vertex = None
        if bloc:
            bloc = O.GiscegisBlocsCtat.read(bloc[0], ['node', 'vertex'])
            if bloc['node']:
                node = bloc['node'][1]
                if bloc['vertex']:
                    v = O.GiscegisVertex.read(bloc['vertex'][0], ['x', 'y'])
                    vertex = (round(v['x'], 3), round(v['y'], 3))
        return node, vertex

    def get_nodes(self, ct_id):
        o = self.connection
        ct = o.GiscedataCts.read(ct_id, ['node_id'])
        if ct.get("node_id"):
            o_node = ct["node_id"][1]
            node = o.GiscegisNodes.read(ct["node_id"][0], ["geom"])
            coords = wkt.loads(node["geom"]).coords[0]
            vertex = [coords[0], coords[1]]
        else:
            o_node, vertex = self.get_node_vertex(ct_id)
        o_node = o_node.replace('*', '')
        return o_node

    def process_trafo(self, trafo_id):
        o = self.connection
        fields_to_read = [
            'ct', 'name', 'cini', 'potencia_nominal', 'id_estat', 'node_id',
            'data_pm', 'data_baixa', 'id_regulatori',
            'tipus_instalacio_cnmc_id', 'node_baixa', 'model'
        ]
        fields_to_read_obra = [
            'subvenciones_europeas', 'subvenciones_nacionales',
            'subvenciones_prtr', 'financiado',
            'fecha_aps', 'fecha_baja', 'causa_baja', 'cuenta_contable',
            'im_ingenieria', 'im_materiales',
            'im_obracivil', 'im_trabajos', 'motivacion', 'tipo_inversion',
            'valor_residual', 'identificador_baja'
        ]

        def get_inst_name(element_id):
            vals = self.connection.GiscedataTransformadorTrafo.read(
                element_id, ['name'])
            return vals['name']

        trafo = o.GiscedataTransformadorTrafo.read(
            trafo_id, fields_to_read
        )

        # MODEL
        if trafo['model']:
            modelo = trafo['model']
        else:
            modelo = ''

        # FECHA_APS
        data_pm = ''
        if trafo['data_pm']:
            data_pm_trafo = datetime.strptime(str(trafo['data_pm']),
                                              '%Y-%m-%d')
            data_pm = data_pm_trafo.strftime('%d/%m/%Y')

        # OBRES
        trafo_obra = ''
        obra_ti_trafo_obj = o.GiscedataProjecteObraTiTransformador
        obra_ti_ids = obra_ti_trafo_obj.search(
            [('element_ti_id', '=', trafo['id'])])
        if obra_ti_ids:
            for obra_ti_id in obra_ti_ids:
                obra_id_data = obra_ti_trafo_obj.read(obra_ti_id, ['obra_id'])
                obra_id = obra_id_data['obra_id']
                # Filtre d'obres finalitzades
                data_finalitzacio_data = o.GiscedataProjecteObra.read(
                    obra_id[0], ['data_finalitzacio'])
                if data_finalitzacio_data:
                    if data_finalitzacio_data.get('data_finalitzacio', False):
                        data_finalitzacio = data_finalitzacio_data[
                            'data_finalitzacio']

                        inici_any = '{}-01-01'.format(self.year)
                        fi_any = '{}-12-31'.format(self.year)
                        if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                            trafo_obra = o.GiscedataProjecteObraTiTransformador.read(
                                obra_ti_id,
                                fields_to_read_obra)
                if trafo_obra:
                    break

        tipo_inversion = ''
        # CAMPS OBRES
        if trafo_obra != '':
            obra_year = data_finalitzacio.split('-')[0]
            data_pm_year = data_pm.split('/')[2]
            if trafo_obra[
                'tipo_inversion'] != '0' and obra_year != data_pm_year:
                data_ip = convert_spanish_date(data_finalitzacio)
            else:
                data_ip = ''
            identificador_baja = (
                get_inst_name(trafo_obra['identificador_baja']) if trafo_obra[
                    'identificador_baja'] else ''
            )
            subvenciones_europeas = format_f_6181(
                trafo_obra['subvenciones_europeas'] or 0.0, float_type='euro')
            subvenciones_nacionales = format_f_6181(
                trafo_obra['subvenciones_nacionales'] or 0.0, float_type='euro')
            subvenciones_prtr = format_f_6181(
                trafo_obra['subvenciones_prtr'] or 0.0, float_type='euro')
            im_ingenieria = format_f_6181(trafo_obra['im_ingenieria'] or 0.0,
                                          float_type='euro')
            im_materiales = format_f_6181(trafo_obra['im_materiales'] or 0.0,
                                          float_type='euro')
            im_obracivil = format_f_6181(trafo_obra['im_obracivil'] or 0.0,
                                         float_type='euro')
            im_trabajos = format_f_6181(trafo_obra['im_trabajos'] or 0.0,
                                        float_type='euro')
            im_construccion = str(format_f(
                float(im_materiales.replace(",", ".")) + float(
                    im_obracivil.replace(",", "."))
                , 2)).replace(".", ",")
            tipo_inversion = trafo_obra['tipo_inversion'] or ''
            valor_auditado = str(
                float(im_construccion.replace(",", ".")) +
                float(im_ingenieria.replace(",", ".")) + float(
                    im_trabajos.replace(",", "."))
            ).replace(".", ",")
            valor_residual = format_f(trafo_obra.get('valor_residual', 0.0), 2)
            cuenta_contable = trafo_obra['cuenta_contable']
            financiado = format_f(trafo_obra.get('financiado', 0.0), 2)
            motivacion = get_codi_actuacio(o, trafo_obra['motivacion'] and
                                           trafo_obra['motivacion'][0])
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
            motivacion = ''
            cuenta_contable = ''
            avifauna = ''
            financiado = ''
            valor_residual = ''

        # Si la data APS es igual a l'any de la generació del fitxer,
        # la data APS sortirà en blanc
        if data_ip:
            data_ip = '' if data_pm and int(data_pm.split('/')[2]) == int(
                data_ip.split('/')[2]) \
                else data_ip

        o_subestacio = trafo['ct'][1]
        o_maquina = (trafo['id_regulatori'] or '').strip() or trafo['name']
        o_cini = trafo['cini']
        o_pot_maquina = format_f(
            float(trafo['potencia_nominal']) / 1000.0, decimals=3)
        o_node = trafo['node_id'] and trafo['node_id'][1] or ''

        if trafo['node_baixa']:
            o_node_baixa = trafo['node_baixa'][1]
        else:
            o_node_baixa = o_node

        if o_cini:
            if o_cini[1] == '2' and o_cini[2] == '4' and o_cini[4] == '0' and \
                    o_cini[7] == '1':
                o_node_baixa = o_node

        # FECHA_BAJA, CAUSA_BAJA

        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        if trafo['data_baixa']:
            if trafo['data_baixa'] < data_pm_limit:
                tmp_date = datetime.strptime(
                    trafo['data_baixa'], '%Y-%m-%d')
                fecha_baja = tmp_date.strftime('%d/%m/%Y')
                if int(fecha_baja.split("/")[2]) - int(
                        data_pm.split("/")[2]) >= 40:
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

        # CODIGO CCUU
        id_ti = trafo['tipus_instalacio_cnmc_id'][0]
        ti = o.GiscedataTipusInstallacio.read(
            id_ti,
            ['name'])['name']

        # ESTADO
        hist_obj = self.connection.model('circular.82021.historics.b5')
        hist_ids = hist_obj.search([
            ('identificador_maquina', '=', o_maquina),
            ('year', '=', self.year - 1)
        ])
        if hist_ids:
            hist = hist_obj.read(hist_ids[0], [
                'cini', 'codigo_ccuu', 'fecha_aps'
            ])
            entregada = F5Res4666(
                cini=hist['cini'],
                codigo_ccuu=hist['codigo_ccuu'],
                fecha_aps=hist['fecha_aps']
            )
            actual = F5Res4666(
                trafo['name'],
                trafo['cini'],
                '',
                ti,
                '',
                '',
                '',
                '',
                data_pm,
                fecha_baja,
                '',
                0
            )
            estado = calculate_estado(fecha_baja, actual, entregada, trafo_obra)
            if estado == '1' and not trafo_obra:
                self.output_m.put("{} {}".format(trafo["name"], adapt_diff(
                    actual.diff(entregada))))
        else:
            estado = default_estado(modelo, data_pm, int(self.year))

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

        # Buidem FECHA_IP si hi ha FECHA_BAJA
        if fecha_baja:
            data_ip = ''

        return [
            o_maquina,  # IDENTIFICADOR_MAQUINA
            o_cini,  # CINI
            ti,  # CCUU
            o_node,  # NUDO_ALTA
            o_node_baixa,  # NUDO_BAJA
            o_pot_maquina,  # POTENCIA MAQUINA
            estado,  # ESTADO
            modelo,  # MODELO
            data_pm,  # FECHA_APS
            fecha_baja,  # FECHA_BAJA
            causa_baja,  # CAUSA_BAJA
            data_ip,  # FECHA_IP
            tipo_inversion,  # TIPO_INVERSION
            im_ingenieria,  # IM_TRAMITES
            im_construccion,  # IM_CONSTRUCCION
            im_trabajos,  # IM_TRABAJOS
            valor_residual,  # VALOR_RESIDUAL
            subvenciones_europeas,  # SUBVENCIONES_EUROPEAS
            subvenciones_nacionales,  # SUBVENCIONES_NACIONALES
            subvenciones_prtr,  # SUBVENCIONES_PRTR
            valor_auditado,  # VALOR_AUDITADO
            financiado,  # FINANCIADO
            cuenta_contable,  # CUENTA
            motivacion,  # MOTIVACION
            identificador_baja,  # IDENTIFICADOR BAJA
        ]

    def process_condensador(self, condensador_id):
        fields_to_read = [
            'ct_id', 'name', 'cini', 'parc_alta', 'parc_baixa', 'data_pm',
            'tipus_instalacio_cnmc_id', 'potencia_instalada', 'model',
            'data_baixa',
        ]
        fields_to_read_obra = [
            'subvenciones_europeas', 'subvenciones_nacionales',
            'subvenciones_prtr', 'financiado',
            'fecha_aps', 'fecha_baja', 'causa_baja', 'cuenta_contable',
            'im_ingenieria', 'im_materiales',
            'im_obracivil', 'im_trabajos', 'motivacion', 'tipo_inversion',
            'valor_residual', 'identificador_baja'
        ]
        condensador = self.connection.GiscedataCondensadors.read(
            condensador_id, fields_to_read
        )

        if condensador['tipus_instalacio_cnmc_id']:
            ti = self.connection.GiscedataTipusInstallacio.read(
                condensador['tipus_instalacio_cnmc_id'][0], ['name']
            )['name']
        else:
            ti = ''

        o_node = ''
        if condensador['ct_id']:
            ct = self.connection.GiscedataCts.read(condensador['ct_id'][0], ['node_id'])
            if ct['node_id']:
                o_node = ct['node_id'][1]

        o_potencia_maquina = format_f(
            float(condensador['potencia_instalada']) / 1000.0, decimals=3
        )

        o_fecha_aps = datetime.strptime(
            condensador['data_pm'], '%Y-%m-%d'
        ).strftime('%d/%m/%Y')

        # Obres
        obra = None
        obres_ti = self.connection.GiscedataProjecteObraTiCondensadors.search([
            ('obra_id.data_finalitzacio', '>=', '{}-01-01'.format(self.year)),
            ('obra_id.data_finalitzacio', '<=', '{}-12-31'.format(self.year))
        ])
        if obres_ti:
            obra = self.connection.GiscedataProjecteObraTiCondensadors.read(
                obres_ti[0], fields_to_read_obra
            )
            data_finalitzacio = self.connection.GiscedataProjecteObra.read(
                obra['obra_id'][0], ['data_finalitzacio']
            )['data_finalitzacio']
            obra_year = data_finalitzacio.split('-')[0]
            data_pm_year = o_fecha_aps.split('/')[2]
            if obra['tipo_inversion'] != '0' and obra_year != data_pm_year:
                data_ip = convert_spanish_date(data_finalitzacio)
            else:
                data_ip = ''
            identificador_baja = obra['identificador_baja'] and obra['identificador_baja'][1] or ''
            subvenciones_europeas = format_f_6181(
                obra['subvenciones_europeas'] or 0.0,
                float_type='euro'
            )
            subvenciones_nacionales = format_f_6181(
                obra['subvenciones_nacionales'] or 0.0,
                float_type='euro'
            )
            subvenciones_prtr = format_f_6181(
                obra['subvenciones_prtr'] or 0.0,
                float_type='euro'
            )
            im_ingenieria = format_f_6181(
                obra['im_ingenieria'] or 0.0,
                float_type='euro'
            )
            im_materiales = format_f_6181(
                obra['im_materiales'] or 0.0,
                float_type='euro'
            )
            im_obracivil = format_f_6181(
                obra['im_obracivil'] or 0.0,
                float_type='euro'
            )
            im_trabajos = format_f_6181(
                obra['im_trabajos'] or 0.0,
                float_type='euro'
            )
            im_construccion = str(format_f(
                float(im_materiales.replace(",", ".")) + float(
                    im_obracivil.replace(",", "."))
                , 2)).replace(".", ",")
            tipo_inversion = obra['tipo_inversion'] or ''
            valor_auditado = str(
                float(im_construccion.replace(",", ".")) +
                float(im_ingenieria.replace(",", ".")) + float(
                    im_trabajos.replace(",", "."))
            ).replace(".", ",")
            valor_residual = format_f(obra.get('valor_residual', 0.0), 2)
            cuenta_contable = obra['cuenta_contable']
            financiado = format_f(obra.get('financiado', 0.0), 2)
            motivacion = get_codi_actuacio(
                self.connection, obra['motivacion'] and obra['motivacion'][0]
            )
        else:
            tipo_inversion = ''
            data_ip = ''
            identificador_baja = ''
            im_ingenieria = ''
            im_construccion = ''
            im_trabajos = ''
            subvenciones_europeas = ''
            subvenciones_nacionales = ''
            subvenciones_prtr = ''
            valor_auditado = ''
            motivacion = ''
            cuenta_contable = ''
            avifauna = ''
            financiado = ''
            valor_residual = ''

        # FECHA_BAJA, CAUSA_BAJA

        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        if condensador['data_baixa']:
            if condensador['data_baixa'] < data_pm_limit:
                tmp_date = datetime.strptime(
                    condensador['data_baixa'], '%Y-%m-%d')
                fecha_baja = tmp_date.strftime('%d/%m/%Y')
                if int(fecha_baja.split("/")[2]) - int(
                        o_fecha_aps.split("/")[2]) >= 40:
                    if identificador_baja != '':
                        causa_baja = 1
                    else:
                        causa_baja = 2
                else:
                    causa_baja = 3
            else:
                fecha_baja = ''
                causa_baja = 0
        else:
            fecha_baja = ''
            causa_baja = 0

        # ESTADO
        hist_obj = self.connection.model('circular.82021.historics.b5')
        hist_ids = hist_obj.search([
            ('identificador_maquina', '=', condensador['name']),
            ('year', '=', self.year - 1)
        ])
        if hist_ids:
            hist = hist_obj.read(hist_ids[0], [
                'cini', 'codigo_ccuu', 'fecha_aps'
            ])
            entregada = F5Res4666(
                cini=hist['cini'],
                codigo_ccuu=hist['codigo_ccuu'],
                fecha_aps=hist['fecha_aps']
            )
            actual = F5Res4666(
                condensador['name'],
                condensador['cini'],
                '',
                ti,
                '',
                '',
                '',
                '',
                o_fecha_aps,
                fecha_baja,
                '',
                0
            )
            estado = calculate_estado(fecha_baja, actual, entregada, obra)
            if estado == '1' and not obra:
                self.output_m.put("{} {}".format(
                    condensador["name"], adapt_diff(actual.diff(entregada))))
        else:
            estado = default_estado(
                condensador['model'], o_fecha_aps, int(self.year))

        if condensador['model'] == 'M':
            estado = ''
            data_pm = ''

        if fecha_baja:
            tipo_inversion = ''

        # L'any 2022 no es declaren subvencions PRTR
        subvenciones_prtr = ''

        if causa_baja == '0':
            fecha_baja = ''

        if condensador['model'] == 'E':
            tipo_inversion = '0'

        # Buidem FECHA_IP si hi ha FECHA_BAJA
        if fecha_baja:
            data_ip = ''

        return [
            condensador['name'],  # IDENTIFICADOR_MAQUINA
            condensador['cini'],  # CINI
            ti,  # CCUU
            o_node,  # NUDO_ALTA
            o_node,  # NUDO_BAJA
            o_potencia_maquina,  # POTENCIA MAQUINA
            estado,  # ESTADO
            condensador['model'],  # MODELO
            o_fecha_aps,  # FECHA_APS
            fecha_baja,  # FECHA_BAJA
            causa_baja,  # CAUSA_BAJA
            data_ip,  # FECHA_IP
            tipo_inversion,  # TIPO_INVERSION
            im_ingenieria,  # IM_TRAMITES
            im_construccion,  # IM_CONSTRUCCION
            im_trabajos,  # IM_TRABAJOS
            valor_residual,  # VALOR_RESIDUAL
            subvenciones_europeas,  # SUBVENCIONES_EUROPEAS
            subvenciones_nacionales,  # SUBVENCIONES_NACIONALES
            subvenciones_prtr,  # SUBVENCIONES_PRTR
            valor_auditado,  # VALOR_AUDITADO
            financiado,  # FINANCIADO
            cuenta_contable,  # CUENTA
            motivacion,  # MOTIVACION
            identificador_baja,  # IDENTIFICADOR BAJA
        ]

    def consumer(self):
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                element_type, element_id = item.split('.')
                if element_type == 'T':
                    result = self.process_trafo(int(element_id))
                elif element_type == 'C':
                    result = self.process_condensador(int(element_id))
                else:
                    result = []
                if result:
                    self.output_q.put(result)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
