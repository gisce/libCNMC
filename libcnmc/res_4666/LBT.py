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
from libcnmc.utils import format_f, tallar_text
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

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        """
        data_pm = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
        search_params = []
        if not self.embarrats:
            search_params += [('cable.tipus.codi', '!=', 'E')]
        search_params += [('propietari', '=', True),
                               ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                               ('data_baixa', '=', False),
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataBtElement.search(
            search_params, 0, 0, False, {'active_test': False})

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
            'data_baixa', self.compare_field
        ]
        data_baixa_limit = '{0}-01-01'.format(self.year)
        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        error_msg = "**** ERROR: l'element {0} (id:{1}) no està en giscegis_edges.\n"
        error_msg_multi = "**** ERROR: l'element {0} (id:{1}) està més d'una vegada a giscegis_edges. {2}\n"
        while True:
            try:
                count += 1
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataBtElement.read(item, fields_to_read)

                res = O.GiscegisEdge.search([('id_linktemplate', '=',
                                              linia['name']),
                                             ('layer', 'ilike', '%BT%')])
                if not res:
                    if not QUIET:
                        sys.stderr.write(
                            error_msg.format(linia['name'], linia['id']))
                        sys.stderr.flush()
                    edge = {'start_node': (0, '{0}_0'.format(linia['name'])),
                            'end_node': (0, '{0}_1'.format(linia['name']))}
                elif len(res) > 1:
                    if not QUIET:
                        sys.stderr.write(
                            error_msg_multi.format(linia['name'], linia['id'], res))
                        sys.stderr.flush()
                    edge = {'start_node': (0, '{0}_0'.format(linia['name'])),
                            'end_node': (0, '{0}_1'.format(linia['name']))}
                else:
                    edge = O.GiscegisEdge.read(res[0], ['start_node',
                                                        'end_node'])
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

                # Descripció
                origen = tallar_text(edge['start_node'][1], 50)
                final = tallar_text(edge['end_node'][1], 50)

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

                if linia[self.compare_field]:
                    last_data = linia[self.compare_field]
                    entregada = F2Res4666(**last_data)
                    actual = F2Res4666(
                        'B{0}'.format(linia['name']),
                        linia['cini'],
                        origen or '',
                        final or '',
                        codi_ccuu or '',
                        comunitat,
                        comunitat,
                        format_f(100.0 - float(linia['perc_financament']),3),
                        data_pm,
                        data_baixa,
                        1,
                        1,
                        format_f(tensio, 3),
                        format_f(longitud, 3),
                        format_f(intensitat),
                        format_f(float(cable['seccio']),2),
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

                output = [
                    'B{}'.format(linia['name']),
                    linia['cini'] or '',
                    origen or '',
                    final or '',
                    codi_ccuu or '',
                    comunitat,
                    comunitat,
                    format_f(100.0 - linia['perc_financament'], 2),
                    data_pm or '',
                    fecha_baja,
                    1,
                    1,
                    format_f(tensio, 3),
                    format_f(longitud, 3),
                    format_f(intensitat, 3),
                    format_f(cable['seccio'], 3),
                    format_f(capacitat, 3),
                    estado
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
