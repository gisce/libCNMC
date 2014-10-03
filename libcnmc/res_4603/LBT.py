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

QUIET = False


class LBT(MultiprocessBased):
    def __init__(self, **kwargs):
        super(LBT, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies BT'
        self.report_name = 'CNMC INVENTARI BT'

    def get_sequence(self):
        search_params = [('baixa', '!=', True), ('cable.tipus.codi', '!=', 'E')]
        return self.connection.GiscedataBtElement.search(search_params)

    def consumer(self):
        O = self.connection
        count = 0
        fields_to_read = ['name', 'municipi', 'data_pm', 'ct',
                          'coeficient', 'cini', 'perc_financament',
                          'longitud_cad', 'cable', 'voltatge', 'data_alta']
        while True:
            try:
                count += 1
                item = self.input_q.get()
                self.progress_q.put(item)
                data_pm = False

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
                    id_comunitat = O.ResComunitat_autonoma.get_ccaa_from_municipi(
                        [], linia['municipi'][0])
                    id_comunitat = id_comunitat[0]
                    comunidad = O.ResComunitat_autonoma.read(id_comunitat,
                                                             ['codi'])
                    if comunidad:
                        comunitat = comunidad['codi']

                if linia['ct']:
                    #Agafar les dates del centrestransformadors
                    cts = O.GiscedataCts.read(linia['ct'][0],
                                              ['data_industria', 'data_pm'])
                    # Calculem any posada en marxa
                    data_pm = linia['data_pm'] or linia['data_alta'] or cts[
                        'data_industria'] or cts['data_pm']

                if data_pm:
                    data_pm = datetime.strptime(str(data_pm), '%Y-%m-%d')
                    data_pm = data_pm.strftime('%d/%m/%Y')

                # Coeficient per ajustar longituds de trams
                coeficient = linia['coeficient'] or 1.0

                # Afagem el tipus de instalacio
                tipus_inst_id = O.Giscedata_cnmcTipo_instalacion.search(
                    [('cini', '=', linia['cini'])])
                codigo = O.Giscedata_cnmcTipo_instalacion.read(
                    tipus_inst_id, ['codi'])
                if codigo:
                    codi = codigo[0]
                else:
                    codi = {'codi': ''}

                # Agafem el cable de la linia
                if linia['cable']:
                    cable = O.GiscedataBtCables.read(linia['cable'][0], [
                        'intensitat_admisible', 'seccio'])
                else:
                    cable = {'seccio': 0, 'intensitat_admisible': 0}

                output = [
                    'B%s' % linia['name'],
                    linia['cini'] or '',
                    edge['start_node'][1] or '',
                    edge['end_node'][1] or '',
                    codi['codi'] or '',
                    comunitat,
                    comunitat,
                    round(100 - int(linia['perc_financament'])),
                    data_pm or '',
                    '',
                    1,
                    1,
                    round(linia['longitud_cad'] * coeficient / 1000.0, 3) or 0,
                    cable['seccio'], round(
                        (cable['intensitat_admisible'] * int(linia['voltatge'])
                         * math.sqrt(3))/1000000, 3)
                ]

                self.output_q.put(output)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()