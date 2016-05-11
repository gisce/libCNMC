#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Posicions
"""
from datetime import datetime
import traceback
import sys

from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_id_municipi_from_company, format_f

QUIET = False


class POS(MultiprocessBased):
    def __init__(self, **kwargs):
        super(POS, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies POS'
        self.report_name = 'CNMC INVENTARI POS'

    def get_sequence(self):
        search_params = [('interruptor', '=', '2')]
        data_pm = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
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
        return self.connection.GiscedataCtsSubestacionsPosicio.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_description(self, pos_id):
        o = self.connection
        pos = o.GiscedataCtsSubestacionsPosicio.read(pos_id, ['subestacio_id'])
        descripcio = ''
        if pos:
            sub_id = pos['subestacio_id'][0]
            sub = o.GiscedataCtsSubestacions.read(sub_id, ['descripcio'])
            if sub:
                descripcio = sub['descripcio']
        return descripcio

    def get_denom(self, sub_id):
        o = self.connection
        res = ''
        ct_id = o.GiscedataCtsSubestacions.read(sub_id, ['ct_id'])['ct_id'][0]
        if ct_id:
            denom = o.GiscedataCts.read(ct_id, ['descripcio'])['descripcio']
            if denom:
                res = denom
        return res

    def consumer(self):
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'data_pm', 'subestacio_id',
            'cnmc_tipo_instalacion', 'perc_financament', 'tensio', 'data_baixa'
        ]
        not_found_msg = '**** ERROR: El ct {0} (id:{1}) no està a giscedata_cts_subestacions_posicio.\n'
        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                pos = O.GiscedataCtsSubestacionsPosicio.read(
                    item, fields_to_read)
                if not pos:
                    txt = (not_found_msg.format(pos['name'], pos['id']))
                    if not QUIET:
                        sys.stderr.write(txt)
                        sys.stderr.flush()

                    raise Exception(txt)
                o_sub = pos['name']
                # Calculem any posada en marxa
                data_pm = pos['data_pm']
                if data_pm:
                    data_pm = datetime.strptime(str(data_pm), '%Y-%m-%d')
                    data_pm = data_pm.strftime('%d/%m/%Y')

                #Codi tipus de instalació
                codigo_ccuu = pos['cnmc_tipo_instalacion']

                comunitat = ''

                #tensio
                ten = O.GiscedataTensionsTensio.read(pos['tensio'][0],
                                                     ['tensio'])
                tensio = (ten['tensio'] / 1000.0) or 0.0

                cts = O.GiscedataCtsSubestacions.read(pos['subestacio_id'][0],
                                                      ['id_municipi'])

                denominacio = self.get_denom(pos['subestacio_id'][0])

                if cts['id_municipi']:
                    id_municipi = cts['id_municipi'][0]
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
                    # o_sub = self.get_description(sub['subestacio_id'][0])
                if pos['data_baixa']:
                    if pos['data_baixa'] < data_pm_limit:
                        tmp_date = datetime.strptime(
                            pos['data_baixa'], '%Y-%m-%d %H:%M:%S')
                        fecha_baja = tmp_date.strftime('%d/%m/%Y')
                    else:
                        fecha_baja = ''
                else:
                    fecha_baja = ''
                if pos['data_pm'] > data_baixa_limit:
                    estado = '2'
                else:
                    estado = '0'
                output = [
                    o_sub,
                    pos['cini'] or '',
                    denominacio,
                    codigo_ccuu,
                    comunitat,
                    format_f(tensio),
                    format_f(round(100 - int(pos['perc_financament']))),
                    data_pm or '',
                    fecha_baja,
                    estado
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
