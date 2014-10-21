#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC BT
"""
from datetime import datetime
from dateutil import parser
import traceback
import math
import sys

from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_id_expedient, tallar_text

QUIET = False


class LBT(MultiprocessBased):
    def __init__(self, **kwargs):
        super(LBT, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies BT'
        self.report_name = 'CNMC INVENTARI BT'

    def get_sequence(self):
        search_params = [('baixa', '=', False), ('cable.tipus.codi', '!=', 'E')]
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
                data_pm = ''
                if linia['data_pm']:
                    data_pm = linia['data_pm']
                else:
                    if linia['ct']:
                        ct_dades = O.GiscedataCts.read(
                            linia['ct'][0], ['expedients_ids', 'data_pm'])
                        id_expedient = get_id_expedient(
                            self.connection, ct_dades['expedients_ids'])
                        if id_expedient:
                            data_exp = O.GiscedataExpedientsExpedient.read(
                                id_expedient, ['industria_data'])
                            if data_exp['industria_data']:
                                data_pm = data_exp['industria_data']

                        if not data_pm and ct_dades['data_pm']:
                            data_pm = ct_dades['data_pm']

                if data_pm:
                    data_pm = parser.parse(data_pm)
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

                #Capacitat
                cap = round(
                    (cable['intensitat_admisible'] * int(linia['voltatge'])
                     * math.sqrt(3))/1000000, 3)
                if cap < 1:
                    capacitat = 1
                else:
                    capacitat = int(round(cap))

                #Descripció
                origen = tallar_text(edge['start_node'], 50)
                final = tallar_text(edge['end_node'], 50)

                output = [
                    'B%s' % linia['name'],
                    linia['cini'] or '',
                    origen or '',
                    final or '',
                    codi['codi'] or '',
                    comunitat,
                    comunitat,
                    round(100 - int(linia['perc_financament'])),
                    data_pm or '',
                    '',
                    1,
                    1,
                    round(linia['longitud_cad'] * coeficient / 1000.0, 3) or 0,
                    cable['seccio'],
                    capacitat

                ]

                self.output_q.put(output)
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()