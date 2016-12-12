#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC BT
"""
from datetime import datetime
import traceback
import math
import sys

from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f, tallar_text
from libcnmc.models.f2_4771 import F2Res4771

QUIET = False


class LBT(MultiprocessBased):
    """
    Class that generates the LBT(2) file of the 4131
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(LBT, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies BT'
        self.report_name = 'CNMC INVENTARI BT'
        self.embarrats = kwargs.pop('embarrats', False)

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
                          '|', ('data_pm', '=', False),
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
        Method that generates the csb file
        :return: List of arrays
        """
        O = self.connection
        count = 0
        fields_to_read = [
            'name', 'municipi', 'data_pm', 'ct', 'coeficient', 'cini',
            'perc_financament', 'longitud_cad', 'cable', 'voltatge',
            'data_alta', 'propietari', 'tipus_instalacio_cnmc_id',
            'data_baixa', '4771_entregada'
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

                # Coeficient per ajustar longituds de trams
                coeficient = linia['coeficient'] or 1.0

                try:
                    tensio = (int(linia['voltatge']) / 1000.0)
                except Exception:
                    tensio = 0.0

                propietari = linia['propietari'] and '1' or '0'

                if linia['tipus_instalacio_cnmc_id']:
                    id_ti = linia.get('tipus_instalacio_cnmc_id')[0]
                    codi_ccuu = O.GiscedataTipusInstallacio.read(
                        id_ti,
                        fields_to_read)['name']
                else:
                    codi_ccuu = ''

                # Agafem el cable de la linia
                if linia['cable']:
                    cable = O.GiscedataBtCables.read(linia['cable'][0], [
                        'intensitat_admisible', 'seccio'])
                else:
                    cable = {'seccio': 0, 'intensitat_admisible': 0}

                intensitat = cable['intensitat_admisible']
                #Capacitat
                capacitat = round(
                    (cable['intensitat_admisible'] * int(linia['voltatge'])
                     * math.sqrt(3)) / 1000, 3)

                if not capacitat:
                    capacitat = 1.0

                #Descripció
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

                if linia['4771_entregada']:
                    data_4771 = linia['4771_entregada']
                    entregada = F2Res4771(**data_4771)
                    actual = F2Res4771(
                        'B{0}'.format(linia['name']),
                        linia['cini'],
                        origen or '',
                        final or '',
                        codi_ccuu or '',
                        comunitat,
                        comunitat,
                        format_f(round(100 - int(linia['perc_financament']))),
                        data_pm,
                        1,
                        1,
                        format_f(tensio),
                        format_f(longitud, 3),
                        format_f(intensitat),
                        format_f(float(cable['seccio']),2),
                        format_f(capacitat),
                        propietari)
                    if actual == entregada:
                        estado = 0
                    else:
                        estado = 1
                else:
                    estado = 2

                output = [
                    'B{}'.format(linia['name']),
                    linia['cini'] or '',
                    origen or '',
                    final or '',
                    codi_ccuu or '',
                    comunitat,
                    comunitat,
                    format_f(round(100 - int(linia['perc_financament']))),
                    data_pm or '',
                    fecha_baja,
                    1,
                    1,
                    format_f(tensio),
                    format_f(longitud, 3),
                    format_f(intensitat),
                    format_f(cable['seccio']),
                    format_f(capacitat),
                    propietari,
                    estado
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
