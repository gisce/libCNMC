#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC BT
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
import math
import sys

from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f, tallar_text, get_forced_elements
from libcnmc.models import F2Res4666

QUIET = False


class LBT(MultiprocessBased):
    """
    Class that generates the LBT(2) file of the 4666
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(LBT, self).__init__(**kwargs)
        self.year = kwargs.pop("year", datetime.now().year - 1)
        self.codi_r1 = kwargs.pop("codi_r1")
        self.base_object = "Línies BT"
        self.report_name = "CNMC INVENTARI BT"
        self.embarrats = kwargs.pop("embarrats", False)
        self.compare_field = kwargs["compare_field"]
        self.extended = kwargs.get("extended", False)
        self.prefix = kwargs.pop('prefix', 'B') or 'B'

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        :rtype: list(int)
        """
        data_pm = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
        search_params = []
        if not self.embarrats:
            search_params += [('cable.tipus.codi', '!=', 'E')]
        search_params += [('propietari', '=', True),
                          ('data_pm', '<', data_pm),
                          '|',
                          '&', ('data_baixa', '>', data_baixa),
                               ('baixa', '=', True),
                          '|',
                               ('data_baixa', '=', False),
                               ('baixa', '=', False)
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        ids = self.connection.GiscedataBtElement.search(
            search_params, 0, 0, False, {'active_test': False})

        forced_ids = get_forced_elements(
            self.connection,
            "giscedata.bt.element"
        )

        ids = ids + forced_ids["include"]
        ids = list(set(ids) - set(forced_ids["exclude"]))

        return list(set(ids))

    def consumer(self):
        """
        Method that generates the csv file
        :return: List of arrays
        """
        O = self.connection
        count = 0
        fields_to_read = [
            'name', 'municipi', 'data_pm', 'ct', 'coeficient', 'cini',
            'perc_financament', 'longitud_cad', 'cable', 'voltatge',
            'data_alta', 'propietari', 'tipus_instalacio_cnmc_id', 'baixa',
            'data_baixa', "edge_id", self.compare_field
        ]
        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        while True:
            try:
                count += 1
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataBtElement.read(item, fields_to_read)

                comunitat = ''
                if linia['municipi']:
                    ccaa_obj = O.ResComunitat_autonoma
                    id_comunitat = ccaa_obj.get_ccaa_from_municipi(
                        linia['municipi'][0])
                    id_comunitat = id_comunitat[0]
                    comunidad = ccaa_obj.read(id_comunitat, ['codi'])
                    if comunidad:
                        comunitat = comunidad['codi']
                data_pm = ''
                if linia['data_pm']:
                    data_pm_linia = datetime.strptime(str(linia['data_pm']),
                                                      '%Y-%m-%d')
                    data_pm = data_pm_linia.strftime('%d/%m/%Y')
                data_baixa = ''
                if linia['baixa'] and linia['data_baixa']:
                    data_baixa = datetime.strptime(str(linia['data_baixa']),
                                                      '%Y-%m-%d')
                    data_baixa = data_baixa.strftime('%d/%m/%Y')

                # Coeficient per ajustar longituds de trams
                coeficient = linia['coeficient'] or 1.0

                try:
                    tensio = (int(linia['voltatge']) / 1000.0)
                except Exception:
                    tensio = 0.0

                if linia['tipus_instalacio_cnmc_id']:
                    id_ti = linia.get('tipus_instalacio_cnmc_id')[0]
                    codi_ccuu = O.GiscedataTipusInstallacio.read(
                        id_ti,
                        ['name'])['name']
                else:
                    codi_ccuu = ''

                # Agafem el cable de la linia
                if linia['cable']:
                    cable = O.GiscedataBtCables.read(linia['cable'][0], [
                        'intensitat_admisible', 'seccio'])
                else:
                    cable = {'seccio': 0, 'intensitat_admisible': 0}

                intensitat = cable['intensitat_admisible']
                # Capacitat
                capacitat = round(
                    (cable['intensitat_admisible'] * int(linia['voltatge'])
                     * math.sqrt(3)) / 1000, 3)

                if not capacitat:
                    capacitat = 1.0

                longitud = round(linia['longitud_cad'] * coeficient / 1000.0,
                                 3) or 0.001
                if linia['data_baixa']:
                    if linia['data_baixa'] > data_pm_limit:
                        fecha_baja = ''
                    else:
                        tmp_date = datetime.strptime(
                            linia['data_baixa'], '%Y-%m-%d')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')
                else:
                    fecha_baja = ''
                last_data = {}
                if linia[self.compare_field]:
                    last_data = linia[self.compare_field]
                    entregada = F2Res4666(**last_data)
                    origen = last_data["origen"]
                    final = last_data["destino"]
                    actual = F2Res4666(
                        '{}{}'.format(self.prefix, linia['name']),
                        linia['cini'],
                        origen or '',
                        final or '',
                        codi_ccuu or '',
                        comunitat,
                        comunitat,
                        format_f(
                            100.0 - linia.get('perc_financament', 0.0), 2
                        ),
                        data_pm,
                        data_baixa,
                        1,
                        1,
                        format_f(tensio, 3),
                        format_f(longitud, 3),
                        format_f(intensitat),
                        format_f(float(cable['seccio']), 2),
                        format_f(capacitat),
                        0
                    )
                    if actual == entregada:
                        estado = 0
                    else:
                        estado = 1
                else:
                    if linia['data_pm']:
                        if linia['data_pm'][:4] != str(self.year):
                            estado = '1'
                        else:
                            estado = '2'
                    else:
                        estado = '1'
                if linia["edge_id"]:
                    edge_id =  linia["edge_id"][0]
                    edge = O.GiscegisEdge.read(edge_id, ['start_node','end_node'])
                    origen = edge['start_node'][1]
                    final = edge['end_node'][1]
                else:
                     origen = last_data["origen"]
                    final = last_data["destino"]

                output = [
                    '{}{}'.format(self.prefix, linia['name']),  # IDENTIFICADOR
                    linia['cini'] or '',            # CINI
                    origen or '',                   # ORIGEN
                    final or '',                    # DESTINO
                    codi_ccuu or '',                # CODIGO_CCUU
                    comunitat,                      # CODIGO_CCAA
                    comunitat,                      # CODIGO_CCAA_2
                    format_f(
                        100.0 - linia.get('perc_financament', 0.0), 2
                    ),                              # FINANCIADO
                    data_pm or '',                  # FECHA_APS
                    fecha_baja,                     # FECHA_BAJA
                    1,                              # NUMERO CIRCUITOS
                    1,                              # NUMERO CONDUCTORES
                    format_f(tensio, 3),            # NIVEL_TENSION
                    format_f(longitud, 3),          # LONGITUD
                    format_f(intensitat, 3),        # INTENSITAT MAXIMA
                    format_f(cable['seccio'], 3),   # SECCION
                    format_f(capacitat, 3),         # CAPACIDAD
                    estado                          # ESTADO
                ]

                if self.extended:

                    if 'municipi' in linia:
                        if linia['municipi']:
                            municipi = O.ResMunicipi.read(
                                linia['municipi'][0], ['name']
                            )
                            output.append(municipi.get('name', ""))
                        else:
                            output.append("")
                    else:
                        output.append("")

                    if 'ct' in linia:
                        if linia['ct']:
                            ct = O.GiscedataCts.read(linia['ct'][0], ['zona_id'])
                            if 'zona_id' in ct:
                                if ct['zona_id']:
                                    zona = O.GiscedataCtsZona.read(
                                        ct['zona_id'][0], ['name']
                                    )
                                    output.append(zona.get('name', ""))
                                else:
                                    output.append("")
                            else:
                                output.append("")
                        else:
                            output.append("")
                    else:
                        output.append("")

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
