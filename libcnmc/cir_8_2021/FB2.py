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
    convert_spanish_date, format_f_6181, get_codi_actuacio, get_ine
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
        self.compare_field = "4666_entregada"
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
            'id', 'name', 'cini', 'data_pm', 'tipus_instalacio_cnmc_id', 'tensio_p',
            'id_municipi', 'perc_financament', 'descripcio', 'data_baixa', 'tensio_const',
            self.compare_field, 'node_baixa', 'zona_id', 'node_id', 'potencia',
            'model','punt_frontera', 'id_regulatori'
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

                # OBRES

                obra_ti_ct_obj = O.GiscedataProjecteObraTiCts
                obra_ti_ct_id = obra_ti_ct_obj.search([('element_ti_id', '=', ct['id'])])
                if obra_ti_ct_id:
                    obra_id_data = obra_ti_ct_obj.read(obra_ti_ct_id[0], ['obra_id'])
                else:
                    obra_id_data = {}

                # Filtre d'obres finalitzades
                ct_obra = ''
                if obra_id_data.get('obra_id', False):
                    obra_id = obra_id_data['obra_id']
                    data_finalitzacio_data = O.GiscedataProjecteObra.read(obra_id[0], ['data_finalitzacio'])
                    if data_finalitzacio_data:
                        if data_finalitzacio_data.get('data_finalitzacio', False):
                            data_finalitzacio = data_finalitzacio_data['data_finalitzacio']

                            inici_any = '{}-01-01'.format(self.year)
                            fi_any = '{}-12-31'.format(self.year)
                            if obra_id and data_finalitzacio and inici_any <= data_finalitzacio <= fi_any:
                                ct_obra = O.GiscedataProjecteObraTiCts.read(obra_ti_ct_id[0], fields_to_read_obra)
                else:
                    ct_obra = ''

                #CAMPS OBRA
                if ct_obra != '':
                    data_ip = convert_spanish_date(
                            ct_obra['fecha_aps'] if ct_obra['tipo_inversion'] != '0' else ''
                    )
                    identificador_baja = (
                        get_inst_name(ct_obra['identificador_baja']) if ct_obra['identificador_baja'] else ''
                    )
                    tipo_inversion = (ct_obra['tipo_inversion'] or '0') if not ct_obra['fecha_baja'] else '1'
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
                    financiado = format_f(ct_obra.get('financiado', 0.0), 2)
                    avifauna = int(ct_obra['avifauna'] == True)
                else:
                    data_ip = ''
                    identificador_baja = ''
                    tipo_inversion = ''
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

                # Fecha APS
                data_pm = ''
                if ct['data_pm']:
                    data_pm_ct = datetime.strptime(str(ct['data_pm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_ct.strftime('%d/%m/%Y')

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

                #DATA_BAIXA, CAUSA_BAJA
                if ct['data_baixa']:
                    if ct['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            ct['data_baixa'], '%Y-%m-%d %H:%M:%S')
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
                        o_node_baixa = '';
                else:
                    o_node_baixa = '';

                #TENSIO
                try:
                    o_tensio_p = format_f(
                        float(ct['tensio_p']) / 1000.0, decimals=3) or ''
                except:
                    o_tensio_p = ''

                #TENSIO_CONST
                if ct['tensio_const']:
                    try:
                        o_tensio_const = format_f(
                            float(ct['tensio_const']) / 1000.0, decimals=3) or ''
                    except:
                        o_tensio_const = ''
                else:
                    o_tensio_const = ''

                #POTENCIA
                o_potencia = str(format_f(
                    float(self.get_potencia_trafos(item)), decimals=3)).replace('.',',')

                #X,Y,Z
                z = ''
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
                if ct[self.compare_field]:
                    last_data = ct[self.compare_field]
                    entregada = F8Res4666(**last_data)

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
                    if entregada == actual and fecha_baja == '':
                        estado = '0'
                    else:
                        self.output_m.put("{} {}".format(ct["name"], adapt_diff(actual.diff(entregada))))
                        estado = '1'
                else:
                    if ct['data_pm']:
                        if ct['data_pm'][:4] != str(self.year):
                            self.output_m.put(
                                "Identificador:{} No estava en el fitxer carregat al any n-1 i la data de PM es diferent al any actual".format(
                                    ct["name"]))
                            estado = '1'
                        else:
                            estado = '2'
                    else:
                        self.output_m.put(
                            "Identificador:{} No estava en el fitxer carregat al any n-1".format(ct["name"]))
                        estado = '1'

                # Fecha APS / Estado
                if modelo == 'M':
                    estado = ''
                    fecha_aps = ''

                output = [
                    o_identificador_ct,           # IDENTIFICADOR
                    ct['cini'] or '',                   # CINI
                    ct['descripcio'] or '',             # DENOMINACION
                    str(ti),                            # CODIGO_CCUU
                    o_node,                             # NUDO_ALTA
                    o_node_baixa,                       # NUDO_BAJA
                    o_tensio_p,                         # NIVEL TENSION
                    o_tensio_const,                     # TENSION CONSTRUCCION
                    o_potencia,                         # POTENCIA
                    format_f(res_srid[0], decimals=3),  # X
                    format_f(res_srid[1], decimals=3),  # Y
                    z,                                  # Z
                    municipio,                          # MUNICIPIO
                    provincia,                          # PROVINCIA
                    comunitat_codi or '',               # CODIGO_CCAA
                    zona_name,                          # ZONA
                    estado,                             # ESTADO
                    modelo,                             # MODELO
                    punto_frontera,                     # PUNTO_FRONTERA
                    data_pm,                            # FECHA APS
                    causa_baja,                         # CAUSA BAJA
                    fecha_baja,                         # FECHA BAJA
                    data_ip,                            # FECHA IP
                    tipo_inversion,                     # TIPO INVERSION
                    im_ingenieria,                      # IM_TRAMITES
                    im_construccion,                    # IM_CONSTRUCCION
                    im_trabajos,                        # IM_TRABAJOS
                    subvenciones_europeas,              # SUBVENCIONES EUROPEAS
                    subvenciones_nacionales,            # SUBVENCIONES NACIONALES
                    subvenciones_prtr,                  # SUBVENCIONES PRTR
                    valor_auditado,                     # VALOR AUDITADO
                    financiado,                         # FINANCIADO
                    cuenta_contable,                    # CUENTA CONTABLE
                    motivacion,                         # MOTIVACION
                    avifauna,                           # AVIFAUNA
                    identificador_baja,                 # ID_BAJA
                ]
                self.output_q.put(output)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
