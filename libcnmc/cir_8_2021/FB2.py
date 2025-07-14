#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback

from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import (
    get_id_municipi_from_company, get_forced_elements, adapt_diff, convert_srid, get_srid, format_f,
    convert_spanish_date, format_f_6181, get_codi_actuacio, get_ine, calculate_estado, default_estado
)
from libcnmc.models import F8Res4666
from shapely import wkt

ZONA = {
    'RURAL CONCENTRADA': 'RC',
    'RURAL DISPERSA': 'RD',
    'URBANA': 'U',
    'SEMIURBANA': 'SU'
}


class FB2(StopMultiprocessBased):
    """
    Class that generates the CT file of the 4666
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(FB2, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'CTS'
        self.report_name = 'Formulario B2: Centros de Transformación'
        self.extended = kwargs.get("extended", False)

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        :rtype: list[int]
        """

        search_params = [('id_installacio.name', '!=', 'SE')]
        data_pm = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
        search_params += [('propietari', '=', True),
                          '|', ('data_pm', '=', False),
                               ('data_pm', '<', data_pm),
                          '|',
                          '&', ('data_baixa', '>', data_baixa),
                               ('ct_baixa', '=', True),
                          '|',
                               ('data_baixa', '=', False),
                               ('ct_baixa', '=', False)
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        # Excloure els registres que es troben de baixa i el model es 'M'
        search_params += [
            '|', ('model', '!=', 'M'), ('data_baixa', '=', False)
        ]

        forced_ids = get_forced_elements(self.connection, "giscedata.cts")

        ids = self.connection.GiscedataCts.search(
            search_params, 0, 0, False, {'active_test': False}
        )

        ids = ids + forced_ids["include"]
        ids = list(set(ids) - set(forced_ids["exclude"]))

        return list(set(ids))

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

    def get_ine(self, municipi_id):
        O = self.connection
        muni = O.ResMunicipi.read(municipi_id, ['ine', 'dc'])
        return get_ine(O, muni['ine'])

    def get_potencia_trafos(self, id_ct):
        o = self.connection
        res = 0
        ids_trafos = o.GiscedataTransformadorTrafo.search([
            ('ct', '=', id_ct), ('id_estat.cnmc_inventari', '=', True)])
        if ids_trafos:
            for elem in ids_trafos:
                trafo = o.GiscedataTransformadorTrafo.read(
                    elem, ['potencia_nominal'])
                if trafo:
                    res += trafo['potencia_nominal']
        return res

    def get_tensio(self, tensio_id):
        o = self.connection
        return o.GiscedataTensionsTensio.read(tensio_id, ['tensio'])['tensio']

    def consumer(self):
        """
        Method that generates the csv file
        :return: List of arrays
        """
        O = self.connection

        def get_inst_name(element_id):
            vals = self.connection.GiscedataCts.read(
                element_id[0], ['name'])
            return vals['name']

        fields_to_read = [
            'id', 'name', 'cini', 'data_pm', 'tipus_instalacio_cnmc_id',
            'tensio_p', 'id_municipi', 'perc_financament', 'descripcio',
            'data_baixa', 'tensio_const', 'node_baixa', 'zona_id', 'node_id',
            'potencia', 'model', 'punt_frontera', 'id_regulatori',
            'perc_financament', 'tensio_entrant'
        ]

        fields_to_read_obra = [
            'name', 'cini', 'tipo_inversion', 'ccuu', 'codigo_ccaa', 'nivel_tension_explotacion', 'financiado',
            'fecha_aps', 'fecha_baja', 'causa_baja', 'im_ingenieria', 'im_materiales', 'im_obracivil',
            'im_trabajos', 'subvenciones_europeas', 'subvenciones_nacionales', 'subvenciones_prtr', 'avifauna',
            'valor_auditado', 'valor_contabilidad', 'cuenta_contable', 'porcentaje_modificacion',
            'motivacion', 'obra_id', 'identificador_baja',
        ]

        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)

        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                ct = O.GiscedataCts.read(item, fields_to_read)

                # IDENTIFICADOR_CT
                if ct.get('id_regulatori', False):
                    o_identificador_ct = ct['id_regulatori']
                else:
                    o_identificador_ct = ct['name']

                # Fecha APS
                data_pm = ''
                if ct['data_pm']:
                    data_pm_ct = datetime.strptime(str(ct['data_pm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_ct.strftime('%d/%m/%Y')

                # OBRES
                ct_obra = ''
                obra_ti_ct_obj = O.GiscedataProjecteObraTiCts
                obra_ti_ids = obra_ti_ct_obj.search([('element_ti_id', '=', ct['id'])])
                if obra_ti_ids:
                    for obra_ti_id in obra_ti_ids:
                        obra_id_data = obra_ti_ct_obj.read(obra_ti_id, ['obra_id'])
                        obra_id = obra_id_data['obra_id']
                        # Filtre d'obres finalitzades
                        data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                        if data_finalitzacio_data:
                            if data_finalitzacio_data.get('data_finalitzacio', False):
                                data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                                inici_any = '{}-01-01'.format(self.year)
                                fi_any = '{}-12-31'.format(self.year)
                                if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                    ct_obra = obra_ti_ct_obj.read(obra_ti_id, fields_to_read_obra)
                        if ct_obra:
                            break

                tipo_inversion = ''
                financiado = ''

                #CAMPS OBRA
                if ct_obra != '':
                    obra_year = data_finalitzacio.split('-')[0]
                    data_pm_year = data_pm.split('/')[2]
                    if ct_obra['tipo_inversion'] != '0' and obra_year != data_pm_year:
                        data_ip = convert_spanish_date(data_finalitzacio)
                    else:
                        data_ip = ''
                    identificador_baja = ''
                    if ct_obra.get('identificador_baja', False):
                        ct_id = ct_obra['identificador_baja'][0]
                        ct_data = O.GiscedataCts.read(ct_id, ['name', 'id_regulatori'])
                        if ct_data.get('id_regulatori', False):
                            identificador_baja = ct_data['id_regulatori']
                        else:
                            identificador_baja = ct_data['name']

                    tipo_inversion = ct_obra['tipo_inversion'] or ''
                    im_ingenieria = format_f_6181(ct_obra['im_ingenieria'] or 0.0, float_type='euro')
                    im_materiales = format_f_6181(ct_obra['im_materiales'] or 0.0, float_type='euro')
                    im_obracivil = format_f_6181(ct_obra['im_obracivil'] or 0.0, float_type='euro')
                    im_construccion = str(format_f(
                        float(im_materiales.replace(",", ".")) + float(im_obracivil.replace(",", "."))
                    , 2)).replace(".", ",")
                    im_trabajos = format_f_6181(ct_obra['im_trabajos'] or 0.0, float_type='euro')
                    subvenciones_europeas = format_f_6181(ct_obra['subvenciones_europeas'] or 0.0, float_type='euro')
                    subvenciones_nacionales = format_f_6181(ct_obra['subvenciones_nacionales'] or 0.0, float_type='euro')
                    subvenciones_prtr = format_f_6181(ct_obra['subvenciones_prtr'] or 0.0, float_type='euro')
                    valor_auditado = format_f_6181(ct_obra['valor_auditado'] or 0.0, float_type='euro')
                    motivacion = get_codi_actuacio(O, ct_obra['motivacion'] and ct_obra['motivacion'][0]) if not \
                        ct_obra['fecha_baja'] else ''
                    cuenta_contable = ct_obra['cuenta_contable'] or ''
                    avifauna = int(ct_obra['avifauna'] == True)
                    causa_baja = ct_obra['causa_baja'] or '0'
                    fecha_baja = ct_obra['fecha_baja'].strftime('%d/%m/%Y') or ''
                    financiado = format_f(ct_obra['financiado'], decimals=2) or ''
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
                    causa_baja = '0'
                    fecha_baja = ''

                # Si la data APS es igual a l'any de la generació del fitxer,
                # la data IP sortirà en blanc
                if data_ip:
                    data_ip = '' if data_pm and int(data_pm.split('/')[2]) == int(data_ip.split('/')[2]) \
                    else data_ip

                #CCAA
                #funció per trobar la ccaa desde el municipi
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                if ct['id_municipi']:
                    id_municipi = ct['id_municipi'][0]
                else:
                    id_municipi = get_id_municipi_from_company(O)
                comunitat_codi = ''
                if id_municipi:
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat_codi = comunitat_vals['codi']

                #CCUU
                if ct['tipus_instalacio_cnmc_id']:
                    id_ti = ct['tipus_instalacio_cnmc_id'][0]
                    ti = O.GiscedataTipusInstallacio.read(
                        id_ti,
                        ['name'])['name']
                else:
                    ti = ''

                #NODE ALTA
                if ct.get("node_id"):
                    o_node = ct["node_id"][1]
                    node = O.GiscegisNodes.read(ct["node_id"][0], ["geom"])
                    coords = wkt.loads(node["geom"]).coords[0]
                    vertex = [coords[0], coords[1]]
                else:
                    o_node, vertex = self.get_node_vertex(item)
                o_node = o_node.replace('*', '')

                #NODE BAIXA
                if ct["node_baixa"]:
                    o_node_baixa = ct["node_baixa"][1]
                    if o_node_baixa == 0:
                        o_node_baixa = ''
                else:
                    o_node_baixa = ''

                if ct['cini']:
                    cini = ct['cini']
                    if o_node and cini[7] == 'V' or cini[7] == 'Z':
                        o_node_baixa = o_node

                #TENSIO
                o_tensio_p = 0.0
                if ct.get('tensio_entrant'):
                    o_tensio_p = float(self.get_tensio(ct['tensio_entrant'][0]))
                elif ct.get('tensio_p'):
                    o_tensio_p = float(ct['tensio_p'])

                #TENSIO_CONST
                o_tensio_const = ''
                if ct.get('tensio_const', False):
                    o_tensio_const = format_f(float(ct['tensio_const'][1]) / 1000.0, decimals=3) or ''

                if o_tensio_const == o_tensio_p:
                    o_tensio_const = ''

                #POTENCIA
                o_potencia = str(format_f(
                    float(self.get_potencia_trafos(item)), decimals=3)).replace('.',',')

                #X,Y,Z
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(get_srid(O), vertex)

                #MUNICIPI I PROVINCIA
                municipio = ''
                provincia = ''
                if ct.get('id_municipi', False):
                    provincia, municipio = self.get_ine(ct['id_municipi'][0])

                #ZONA
                if 'zona_id' in ct and ct['zona_id']:
                    zona = O.GiscedataCtsZona.read(
                        ct['zona_id'][0], ['name']
                    )
                    tmp_zona = zona.get('name', "")
                    zona_name = ZONA[tmp_zona.upper()]
                else:
                    zona_name = ""

                #PUNT_FRONTERA
                punto_frontera = int(ct['punt_frontera'] == True)

                #MODELO
                modelo = ''
                if ct.get('model', False):
                    modelo = ct['model']

                # Estado
                hist_obj = O.model('circular.82021.historics.b2')
                hist_ids = hist_obj.search([
                    ('identificador_ct', '=', o_identificador_ct),
                    ('year', '=', self.year - 1)
                ])
                if hist_ids:
                    hist = hist_obj.read(hist_ids[0], [
                        'cini', 'codigo_ccuu', 'fecha_aps'
                    ])
                    entregada = F8Res4666(
                        cini=hist['cini'],
                        codigo_ccuu=hist['codigo_ccuu'],
                        fecha_aps=hist['fecha_aps']
                    )

                    id_ti = ct['tipus_instalacio_cnmc_id'][0]
                    ti = O.GiscedataTipusInstallacio.read(
                        id_ti,
                        ['name'])['name']

                    actual = F8Res4666(
                        ct['name'],
                        ct['cini'],
                        '',
                        ti,
                        comunitat_codi,
                        '',
                        data_pm,
                        fecha_baja,
                        0
                    )
                    estado = calculate_estado(
                        fecha_baja, actual, entregada, ct_obra)
                    if estado == '1' and not ct_obra:
                        self.output_m.put("{} {}".format(ct["name"], adapt_diff(
                            actual.diff(entregada))))
                else:
                    estado = default_estado(modelo, data_pm, int(self.year))

                # Fecha APS / Estado
                if modelo == 'M':
                    estado = ''
                    data_pm = ''

                if fecha_baja:
                    tipo_inversion = ''

                # L'any 2022 no es declaren subvencions PRTR
                subvenciones_prtr = ''

                if causa_baja == '0':
                    fecha_baja = ''

                if modelo == 'E' and estado == '2':
                    tipo_inversion = '0'

                # Buidem FECHA_IP si hi ha FECHA_BAJA
                if fecha_baja:
                    data_ip = ''

                output = [
                    o_identificador_ct,                                 # IDENTIFICADOR
                    ct['cini'] or '',                                   # CINI
                    ct['descripcio'] or '',                             # DENOMINACION
                    str(ti),                                            # CODIGO_CCUU
                    o_node,                                             # NUDO_ALTA
                    o_node_baixa,                                       # NUDO_BAJA
                    format_f(o_tensio_p / 1000.0, decimals=3) or '',    # NIVEL TENSION
                    o_tensio_const,                                     # TENSION CONSTRUCCION
                    o_potencia,                                         # POTENCIA
                    format_f(res_srid[0], decimals=3),                  # X
                    format_f(res_srid[1], decimals=3),                  # Y
                    '0,000',                                            # Z
                    municipio,                                          # MUNICIPIO
                    provincia,                                          # PROVINCIA
                    comunitat_codi or '',                               # CODIGO_CCAA
                    zona_name,                                          # ZONA
                    estado,                                             # ESTADO
                    modelo,                                             # MODELO
                    punto_frontera,                                     # PUNTO_FRONTERA
                    data_pm,                                            # FECHA APS
                    causa_baja,                                         # CAUSA BAJA
                    fecha_baja,                                         # FECHA BAJA
                    data_ip,                                            # FECHA IP
                    tipo_inversion,                                     # TIPO INVERSION
                    im_ingenieria,                                      # IM_TRAMITES
                    im_construccion,                                    # IM_CONSTRUCCION
                    im_trabajos,                                        # IM_TRABAJOS
                    subvenciones_europeas,                              # SUBVENCIONES EUROPEAS
                    subvenciones_nacionales,                            # SUBVENCIONES NACIONALES
                    subvenciones_prtr,                                  # SUBVENCIONES PRTR
                    valor_auditado,                                     # VALOR AUDITADO
                    financiado,                                         # FINANCIADO
                    cuenta_contable,                                    # CUENTA CONTABLE
                    motivacion,                                         # MOTIVACION
                    avifauna,                                           # AVIFAUNA
                    identificador_baja,                                 # ID_BAJA
                ]
                self.output_q.put(output)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
