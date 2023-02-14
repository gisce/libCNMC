#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Condensadors
"""
from __future__ import absolute_import
import sys
from datetime import datetime
import traceback
from operator import itemgetter

from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import format_f, get_forced_elements


class CON(StopMultiprocessBased):
    """
    Class that generates the Maquinas/Condensadores(5) file of the 4666
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(CON, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies CON'

        tension_fields_to_read = ['l_inferior', 'l_superior', 'tensio']
        tension_vals = self.connection.GiscedataTensionsTensio.read(
            self.connection.GiscedataTensionsTensio.search([]),
            tension_fields_to_read)

        self.tension_norm = [(t['l_inferior'], t['l_superior'], t['tensio'])
                             for t in tension_vals]
        t_norm_txt = ''
        for t in sorted(self.tension_norm, key=itemgetter(2)):
            t_norm_txt += '[{0:6d} <= {2:6d} < {1:6d}]\n'.format(*t)
        sys.stderr.write('Tensions normalitzades: \n{0}'.format(t_norm_txt))
        sys.stderr.flush()
        self.report_name = 'CNMC INVENTARI CON'

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer

        :return: List of ids to pass to the consumer
        :rtype: list[int]
        """
        data_pm = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
        search_params = [('propietari', '=', True),
                         '|', ('data_pm', '=', False),
                         ('data_pm', '<', data_pm),
                         '|', ('data_baixa', '>', data_baixa),
                         ('data_baixa', '=', False)
                         ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                          ('data_baixa', '!=', False),
                          ('active', '=', True)]

        forced_ids = get_forced_elements(
            self.connection,
            "giscedata.condensadors"
        )

        ids_condensadors = self.connection.GiscedataCondensadors.search(
            search_params, 0, 0, False, {'active_test': False})

        ids_condensadors = ids_condensadors + forced_ids["include"]
        ids_condensadors = list(set(ids_condensadors) - set(forced_ids["exclude"]))

        return list(set(ids_condensadors))

    def get_norm_tension(self, tension):
        """
        Method that gives the tension normalizada
        :param tension: tension as int
        :return: Tension normalizada
        """
        if not tension:
            return tension

        for t in self.tension_norm:
            if t[0] <= tension < t[1]:
                return t[2]

        sys.stderr.write('WARN: Tensió inexistent: {0}\n'.format(tension))
        sys.stderr.flush()
        return tension

    def consumer(self):
        """
        Method that generates the csv file
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'cini', 'data_pm', 'ct_id', 'name', 'potencia_nominal', 'tensio_id',
            'numero_fabricacio', 'participacio', 'potencia_instalada',
            'tipus_instalacio_cnmc_id', 'conexions', 'data_baixa']

        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)
        while True:
            try:
                item = self.input_q.get()
                if item == "STOP":
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                cond = O.GiscedataCondensadors.read(item, fields_to_read)
                codigo_ccuu = O.GiscedataTipusInstallacio.read(
                    cond['tipus_instalacio_cnmc_id'][0], ['name'])['name']

                data_pm = ''
                if cond['data_pm']:
                    data_pm = datetime.strptime(
                        str(cond['data_pm']), '%Y-%m-%d')
                    data_pm = data_pm.strftime('%d/%m/%Y')

                comunitat = ''
                financiacio = round(100.0 - float(cond['participacio']), 2)

                id_municipi = ''
                sys.stderr.write('CT {0} -> '.format(cond['ct_id']))
                denominacio = cond['ct_id']
                if cond['ct_id']:
                    cts = O.GiscedataCts.read(cond['ct_id'][0],
                                              ['id_municipi', 'descripcio'])
                    if cts['id_municipi']:
                        id_municipi = cts['id_municipi'][0]
                    denominacio = cts['descripcio']

                if id_municipi:
                    fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                    id_comunitat = fun_ccaa(id_municipi)
                    comunidad = O.ResComunitat_autonoma.read(
                        id_comunitat, ['codi'])
                    comunitat = comunidad[0]['codi']
                tensio_primari = O.GiscedataTensionsTensio.read(
                    cond['tensio_id'][0], ['tensio'])['tensio']
                tensio_secundari = tensio_primari
                if cond['data_baixa']:
                    if cond['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            cond['data_baixa'], '%Y-%m-%d')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')
                    else:
                        fecha_baja = ''
                else:
                    fecha_baja = ''
                if cond['data_pm'] > data_baixa_limit:
                    estado = '2'
                elif fecha_baja != '':
                    estado = '1'
                else:
                    estado = '0'
                capacidad = format_f(cond['potencia_instalada']/1000, 3)
                output = [
                    '{0}'.format(cond['name']),
                    cond['cini'] or '',
                    denominacio or '',
                    codigo_ccuu,
                    comunitat or '',
                    format_f(float(tensio_primari)/1000.0),
                    format_f(float(tensio_secundari)/1000.0),
                    format_f(financiacio, 2),
                    data_pm,
                    fecha_baja,
                    capacidad,
                    estado
                ]

                self.output_q.put(output)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
