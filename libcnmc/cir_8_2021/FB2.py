#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Centres Transformadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.utils import (
    format_f, get_id_municipi_from_company, get_forced_elements, adapt_diff, convert_srid, get_srid, format_f,
    convert_spanish_date, get_name_ti, format_f_6181, get_codi_actuacio
)
from libcnmc.models import F8Res4666
from shapely import wkt


class FB2(MultiprocessBased):
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
        Method that generates the csb file
        :return: List of arrays
        """
        O = self.connection

        def get_inst_name(element_id):
            vals = self.connection.GiscedataCts.read(
                element_id, ['name'])
            return vals['name']

        fields_to_read = [
            'name', 'cini', 'data_pm', 'tipus_instalacio_cnmc_id', 'tensio_p',
            'id_municipi', 'perc_financament', 'descripcio', 'data_baixa',
            self.compare_field, 'id_provincia', 'zona_id', 'node_id', 'potencia'
        ]

        fields_to_read_obra = [
            'name',
            'cini',
            'tipo_inversion',
            'ccuu',
            'codigo_ccaa',
            'nivel_tension_explotacion',
            'financiado',
            'fecha_aps',
            'fecha_baja',
            'causa_baja',
            'im_ingenieria',
            'im_materiales',
            'im_obracivil',
            'im_trabajos',
            'subvenciones_europeas',
            'subvenciones_nacionales',
            'valor_auditado',
            'valor_contabilidad',
            'cuenta_contable',
            'porcentaje_modificacion',
            'motivacion',
            'obra_id',
            'identificador_baja',
        ]

        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                ct = O.GiscedataCts.read(item, fields_to_read)

                print("item + " + item)
                print(O.GiscedataProjecteObraTiCts.read(item, fields_to_read_obra))

                linia = O.GiscedataProjecteObraTiCts.read(item, fields_to_read_obra)[0]

                data_ip = convert_spanish_date(
                    linia['fecha_aps'] if not linia['fecha_baja']
                                          and linia['tipo_inversion'] != '1' else ''
                )
                # Si la data APS es igual a l'any de la generació del fitxer,
                # la data APS sortirà en blanc
                data_ip = '' if data_ip and int(data_ip.split('/')[2]) != self.year \
                    else data_ip

                comunitat_codi = ''
                data_pm = ''

                if ct['data_pm']:
                    data_pm_ct = datetime.strptime(str(ct['data_pm']),
                                                   '%Y-%m-%d')
                    data_pm = data_pm_ct.strftime('%d/%m/%Y')

                #funció per trobar la ccaa desde el municipi
                fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi

                if ct['id_municipi']:
                    id_municipi = ct['id_municipi'][0]
                else:
                    id_municipi = get_id_municipi_from_company(O)

                if id_municipi:
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat_codi = comunitat_vals['codi']

                identificador_baja = (
                    get_inst_name(linia['identificador_baja'][0])
                    if linia['identificador_baja'] else ''
                ),

                if ct['data_baixa']:
                    if ct['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            ct['data_baixa'], '%Y-%m-%d %H:%M:%S')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')

                        if data_pm - fecha_baja >= 40:
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
                        ct['descripcio'],
                        ti,
                        comunitat_codi,
                        format_f(
                            100.0 - ct.get('perc_financament', 0.0), 2
                        ),
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
                            self.output_m.put("Identificador:{} No estava en el fitxer carregat al any n-1 i la data de PM es diferent al any actual".format(ct["name"]))
                            estado = '1'
                        else:
                            estado = '2'
                    else:
                        self.output_m.put("Identificador:{} No estava en el fitxer carregat al any n-1".format(ct["name"]))
                        estado = '1'
                if ct['tipus_instalacio_cnmc_id']:
                    id_ti = ct['tipus_instalacio_cnmc_id'][0]
                    ti = O.GiscedataTipusInstallacio.read(
                        id_ti,
                        ['name'])['name']

                else:
                    ti = ''

                if ct.get("node_id"):
                    o_node = ct["node_id"][1]
                    node = O.GiscegisNodes.read(ct["node_id"][0], ["geom"])
                    coords = wkt.loads(node["geom"]).coords[0]
                    vertex = [coords[0], coords[1]]
                else:
                    o_node, vertex = self.get_node_vertex(item)
                o_node = o_node.replace('*', '')

                try:
                    o_tensio_p = format_f(
                        float(ct['tensio_p']) / 1000.0, decimals=3) or ''
                except:
                    o_tensio_p = ''

                o_potencia = float(self.get_potencia_trafos(item))

                z = ''
                res_srid = ['', '']
                if vertex:
                    res_srid = convert_srid(get_srid(O), vertex)

                if ct['id_municipi']:
                    o_ine_prov, o_ine_muni = self.get_ine(ct['id_municipi'][0])
                else:
                    o_ine_muni, o_ine_prov = '', ''

                tipo_inversion = (linia['tipo_inversion'] or '0') if not linia['fecha_baja'] else '1', get_name_ti(
                    O, linia['ccuu'] and linia['ccuu'][0])

                im_ingenieria = format_f_6181(linia['im_ingenieria'] or 0.0, float_type='euro')

                im_materiales = format_f_6181(linia['im_materiales'] or 0.0, float_type='euro'),
                im_obracivil = format_f_6181(linia['im_obracivil'] or 0.0, float_type='euro'),

                im_construccion = im_materiales + im_obracivil

                im_trabajos = format_f_6181(linia['im_trabajos'] or 0.0, float_type='euro')

                subvenciones_europeas = format_f_6181(linia['subvenciones_europeas'] or 0.0, float_type='euro'),
                subvenciones_nacionales = format_f_6181(linia['subvenciones_nacionales'] or 0.0, float_type='euro'),

                valor_auditado = format_f_6181(linia['valor_auditado'] or 0.0, float_type='euro'),

                motivacion = get_codi_actuacio(O, linia['motivacion'] and linia['motivacion'][0]) if not \
                    linia['fecha_baja'] else '',


                output = [
                    '{0}'.format(ct['name']),           # IDENTIFICADOR
                    ct['cini'] or '',                   # CINI
                    ct['descripcio'] or '',             # DENOMINACION
                    str(ti),                            # CODIGO_CCUU
                    o_node,                             # NUDO_ALTA
                    o_tensio_p,                         # NIVEL TENSION
                    o_potencia,                         # POTENCIA
                    format_f(res_srid[0], decimals=3),  # X
                    format_f(res_srid[1], decimals=3),  # Y
                    z,                                  # Z
                    o_ine_muni,                         # MUNICIPIO
                    o_ine_prov,                         # PROVINCIA
                    comunitat_codi or '',               # CODIGO_CCAA
                    estado,                             # ESTADO
                    data_pm,                            # FECHA APS
                    causa_baja,                         # CAUSA BAJA
                    data_ip,                            # FECHA IP
                    tipo_inversion,                     # TIPO INVERSION
                    im_ingenieria,                      # IM_TRAMITES
                    im_construccion,                    # IM_CONSTRUCCION
                    im_trabajos,                        # IM_TRABAJOS
                    subvenciones_europeas,              # SUBVENCIONES EUROPEAS
                    subvenciones_nacionales,            # SUBVENCIONES NACIONALES
                    valor_auditado,                     # VALOR AUDITADO
                    format_f(
                        100.0 - ct.get('perc_financament', 0.0), 2
                    ),                                  # FINANCIADO
                    linia['cuenta_contable'],           # CUENTA CONTABLE
                    motivacion,                         # MOTIVACION
                    identificador_baja                  # ID_BAJA

                ]
                if self.extended:

                    if 'id_provincia' in ct:
                        provincia = O.ResCountryState.read(
                            ct['id_provincia'][0], ['name']
                        )
                        output.append(provincia.get('name', ""))
                    else:
                        output.append("")

                    if 'id_municipi' in ct:
                        municipi = O.ResMunicipi.read(
                            ct['id_municipi'][0], ['name']
                        )
                        output.append(municipi.get('name', ""))
                    else:
                        output.append("")

                    if 'zona_id' in ct:
                        zona = O.GiscedataCtsZona.read(
                            ct['zona_id'][0], ['name']
                        )
                        output.append(zona.get('name', ""))
                    else:
                        output.append("")

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
