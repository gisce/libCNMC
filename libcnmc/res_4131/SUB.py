#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Subestacions
"""
from datetime import datetime
import traceback
import sys

from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_id_municipi_from_company, format_f
from libcnmc.models.f3_4771 import F3Res4771

QUIET = False


class SUB(MultiprocessBased):
    """
    Class that generates the SUB(3) report of the 4131
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(SUB, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Subestacions'
        self.report_name = 'CNMC INVENTARI SUB'

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        """
        search_params = []
        data_pm = '{}-01-01'.format(self.year + 1)
        data_baixa = '{}-01-01'.format(self.year)
        search_params += [('propietari', '=', True),
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
        return self.connection.GiscedataCtsSubestacions.search(
            search_params, 0, 0, False, {'active_test': False})

    def consumer(self):
        """
        Method that generates the csb file
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'name', 'data_industria', 'data_pm', 'id_municipi', 'cini',
            'descripcio', 'perc_financament', 'data_baixa', 'posicions',
            'cnmc_tipo_instalacion', '4771_entregada'
        ]
        data_pm_limit = '{}-01-01'.format(self.year + 1)
        data_baixa_limit = '{}-01-01'.format(self.year)
        error_msg = "**** ERROR: El ct %s (id:%s) no està en giscedata_cts_subestacions.\n"
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                sub = O.GiscedataCtsSubestacions.read(item, fields_to_read)

                if not sub:
                    txt = (error_msg.format((sub['name'], sub['id'])))
                    if not QUIET:
                        sys.stderr.write(txt)
                        sys.stderr.flush()

                    raise Exception(txt)

                # Calculem any posada en marxa
                data_pm = sub['data_pm']

                if data_pm:
                    data_pm = datetime.strptime(str(data_pm), '%Y-%m-%d')
                    data_pm = data_pm.strftime('%d/%m/%Y')

                comunitat = ''
                if sub['id_municipi']:
                    id_municipi = sub['id_municipi'][0]
                else:
                    id_municipi = get_id_municipi_from_company(O)

                if id_municipi:
                    #funció per trobar la ccaa desde el municipi
                    fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
                    id_comunitat = fun_ccaa(id_municipi)
                    comunitat_vals = O.ResComunitat_autonoma.read(
                        id_comunitat[0], ['codi'])
                    if comunitat_vals:
                        comunitat = comunitat_vals['codi']
                if sub['data_baixa']:
                    if sub['data_baixa'] < data_pm_limit:
                        fecha_baja = sub['data_baixa']
                    else:
                        fecha_baja = ''
                else:
                    fecha_baja = ''

                if 'posicions' in sub:
                    num_pos = 0
                    for pos in sub['posicions']:
                        pos_data = O.GiscedataCtsSubestacionsPosicio.read(pos, ['interruptor'])
                        if pos_data['interruptor'] == '2':
                            num_pos += 1
                else:
                    num_pos = 1

                if sub['4771_entregada']:
                    data_4771 = sub['4771_entregada']
                    entregada = F3Res4771(**data_4771)
                    actual = F3Res4771(
                        sub['name'],
                        sub['cini'],
                        sub['descripcio'],
                        comunitat,
                        format_f(round(100 - int(sub['perc_financament']))),
                        data_pm,
                        num_pos
                    )
                    if entregada == actual:
                        estado = 0
                    else:
                        estado = 1
                else:
                    estado = 2
                    
                output = [
                    '{}'.format(sub['name']),
                    sub['cini'] or '',
                    sub['descripcio'] or '',
                    comunitat,
                    format_f(round(100 - int(sub['perc_financament']))),
                    data_pm,
                    fecha_baja,
                    num_pos,
                    estado
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
