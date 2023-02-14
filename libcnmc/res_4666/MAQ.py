#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Maquines
"""
from __future__ import absolute_import
import sys
from datetime import datetime
import traceback
from operator import itemgetter

from libcnmc.core import StopMultiprocessBased
from libcnmc.utils import \
    (get_id_municipi_from_company, format_f, get_forced_elements, adapt_diff)
from libcnmc.models import F5Res4666


class MAQ(StopMultiprocessBased):
    """
    Class that generates the Maquinas/Transofrmadores(5) file of the 4666
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(MAQ, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies MAQ'
        self.compare_field = kwargs["compare_field"]

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
        self.report_name = 'CNMC INVENTARI MAQ'

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer

        :return: List of ids
        :rtype: list(int)
        """

        data_pm = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
        search_params = [
            ('propietari', '=', True),
            '|', ('data_pm', '=', False),
            ('data_pm', '<', data_pm),
            '|',
            '&', ('data_baixa', '>', data_baixa),
                 ('baixa', '=', True),
            '|',
                 ('data_baixa', '=', False),
                 ('baixa', '=', False)
        ]

        # Transformadors reductors
        search_params_reductor = search_params + [
            ('id_estat.cnmc_inventari', '=', True),
            ('reductor', '=', True)]

        # Transformadors
        search_params_transformadors = search_params + [
            ('id_estat.cnmc_inventari', '=', True),
            ('localitzacio.code', '=', '1'),
            ('id_estat.codi', '!=', '1')]

        #search_params_reductor += search_params
        ids_reductor = self.connection.GiscedataTransformadorTrafo.search(
            search_params_reductor, 0, 0, False, {'active_test': False})

        ids_transformadors = self.connection.GiscedataTransformadorTrafo.search(
            search_params_transformadors, 0, 0, False, {'active_test': False})

        ids = list(set(ids_reductor + ids_transformadors))

        forced_ids = get_forced_elements(
            self.connection,
            "giscedata.transformador.trafo"
        )

        ids = ids + forced_ids["include"]
        ids = list(set(ids) - set(forced_ids["exclude"]))

        return list(set(ids))

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
        Method that generates the csb file
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'cini', 'historic', 'data_pm', 'ct', 'name', 'potencia_nominal',
            'numero_fabricacio', 'perc_financament', 'tipus_instalacio_cnmc_id',
            'conexions', 'data_baixa', self.compare_field
        ]

        con_fields_to_read = ['conectada', 'tensio_primari', 'tensio_p2',
                              'tensio_b1', 'tensio_b2', 'tensio_b3']

        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)
        while True:
            try:
                item = self.input_q.get()
                if item == "STOP":
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)

                trafo = O.GiscedataTransformadorTrafo.read(
                    item, fields_to_read)

                if trafo['tipus_instalacio_cnmc_id']:
                    id_ti = trafo.get('tipus_instalacio_cnmc_id')[0]
                    codigo_ccuu = O.GiscedataTipusInstallacio.read(
                        id_ti,
                        ['name'])['name']
                else:
                    codigo_ccuu = ''

                data_pm = ''
                if trafo['data_pm']:
                    data_pm = datetime.strptime(
                        str(trafo['data_pm']), '%Y-%m-%d')
                    data_pm = data_pm.strftime('%d/%m/%Y')

                comunitat = ''
                financiacio = format_f(
                    100.0 - trafo.get('perc_financament', 0.0), 2
                )

                # Unitats en MVA
                capacitat = trafo['potencia_nominal'] / 1000.0

                id_municipi = ''
                sys.stderr.write('CT {0} -> '.format(trafo['ct']))
                if trafo['ct']:
                    cts = O.GiscedataCts.read(trafo['ct'][0],
                                              ['id_municipi', 'descripcio'])
                    if cts['id_municipi']:
                        id_municipi = cts['id_municipi'][0]
                    denominacio = cts['descripcio']
                else:
                    id_municipi = get_id_municipi_from_company(O)
                    denominacio = 'ALMACEN'

                if id_municipi:
                    fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                    id_comunitat = fun_ccaa(id_municipi)
                    comunidad = O.ResComunitat_autonoma.read(
                        id_comunitat, ['codi'])
                    comunitat = comunidad[0]['codi']

                #Càlcul tensions a partir de les connexions
                con_vals = O.GiscedataTransformadorConnexio.read(
                    trafo['conexions'], con_fields_to_read)

                tensio_primari = 0
                tensio_secundari = 0
                for con in con_vals:
                    if not con['conectada']:
                        continue
                    t_prim = max([con['tensio_primari'] or 0,
                                  con['tensio_p2'] or 0])
                    t_sec = max([con['tensio_b1'] or 0,
                                 con['tensio_b2'] or 0,
                                 con['tensio_b3'] or 0])
                    tensio_primari = self.get_norm_tension(t_prim) / 1000.0
                    tensio_secundari = self.get_norm_tension(t_sec) / 1000.0
                if trafo['data_baixa']:
                    if trafo['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            trafo['data_baixa'], '%Y-%m-%d %H:%M:%S')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')
                    else:
                        fecha_baja = ''
                else:
                    fecha_baja = ''

                if trafo[self.compare_field]:
                    last_data = trafo[self.compare_field]
                    entregada = F5Res4666(**last_data)
                    actual = F5Res4666(
                        trafo['name'],
                        trafo['cini'],
                        denominacio,
                        codigo_ccuu,
                        comunitat,
                        format_f(tensio_primari),
                        format_f(tensio_secundari),
                        financiacio,
                        data_pm,
                        fecha_baja,
                        format_f(capacitat, 3),
                        0
                    )
                    if entregada == actual and fecha_baja == '':
                        estado = '0'
                    else:
                        self.output_m.put("{} {}".format(trafo["name"], adapt_diff(actual.diff(entregada))))
                        estado = '1'
                else:
                    if trafo['data_pm']:
                        if trafo['data_pm'][:4] != str(self.year):
                            self.output_m.put("Identificador:{} No estava en el fitxer carregat al any n-1 i la data de PM es diferent al any actual".format(trafo["name"]))
                            estado = '1'
                        else:
                            estado = '2'
                    else:
                        self.output_m.put("Identificador:{} No estava en el fitxer carregat al any n-1".format(trafo["name"]))
                        estado = '1'

                output = [
                    '{0}'.format(trafo['name']),        # IDENTIFICADOR
                    trafo['cini'] or '',                # CINI
                    denominacio or '',                  # DENOMINACION
                    codigo_ccuu,                        # CODIGO_CCUU
                    comunitat or '',                    # CODIGO_CCAA
                    format_f(tensio_primari, 3),        # TENSION PRIMARIO
                    format_f(tensio_secundari, 3),      # TENSION SECUNDARIO
                    financiacio,                        # FINANCIADO
                    data_pm,                            # FECHA APS
                    fecha_baja,                         # FECHA BAJA
                    format_f(capacitat, 3),             # CAPACIDAD
                    estado                              # ESTADO
                ]

                self.output_q.put(output)
                self.input_q.task_done()
            except Exception:
                self.input_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
