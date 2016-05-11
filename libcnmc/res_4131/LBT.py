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

QUIET = False


class LBT(MultiprocessBased):
    def __init__(self, **kwargs):
        super(LBT, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies BT'
        self.report_name = 'CNMC INVENTARI BT'
        self.embarrats = kwargs.pop('embarrats', False)

    def get_sequence(self):

        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-01-01' % self.year
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
        O = self.connection
        count = 0
        fields_to_read = [
            'name', 'municipi', 'data_pm', 'ct','coeficient', 'cini',
            'perc_financament', 'longitud_cad', 'cable', 'voltatge',
            'data_alta', 'propietari', 'cnmc_tipo_instalacion',
            'data_baixa'
        ]
        data_baixa_limit = '%s-01-01' % self.year
        data_pm_limit = '%s-01-01' % str( self.year + 1)
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
                            "**** ERROR: l'element %s (id:%s) no està en "
                            "giscegis_edges.\n" % (linia['name'], linia['id']))
                        sys.stderr.flush()
                    edge = {'start_node': (0, '%s_0' % linia['name']),
                            'end_node': (0, '%s_1' % linia['name'])}
                elif len(res) > 1:
                    if not QUIET:
                        sys.stderr.write("**** ERROR: l'element %s (id:%s) "
                                         "està més d'una vegada a "
                                         "giscegis_edges. %s\n" %
                                         (linia['name'], linia['id'], res))
                        sys.stderr.flush()
                    edge = {'start_node': (0, '%s_0' % linia['name']),
                            'end_node': (0, '%s_1' % linia['name'])}
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
                except:
                    tensio = 0.0

                propietari = linia['propietari'] and '1' or '0'

                codi_ccuu = linia['cnmc_tipo_instalacion']
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
                if linia['data_pm']:
                    if linia['data_pm'] > data_baixa_limit:
                        estado = 2
                    else:
                        estado = 0
                else:
                    estado = 0

                output = [
                    'B%s' % linia['name'],
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
