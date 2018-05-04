#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Posicions
"""
from __future__ import absolute_import
from datetime import datetime
import traceback
import sys

from libcnmc.core import MultiprocessBased
from libcnmc.utils import get_id_municipi_from_company, format_f
from libcnmc.models import F4Res4666

QUIET = False


class POS(MultiprocessBased):
    """
    Class that generates the POS/Interruptores(4) of 4666 report
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(POS, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies POS'
        self.report_name = 'CNMC INVENTARI POS'
        self.compare_field = kwargs["compare_field"]
        self.extended = kwargs.get("extended", False)

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        """
        search_params = [('cini', 'ilike', 'i28_2%')]
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
        """
        Method that gives the description
        :param pos_id: Posicio id
        :return: Position description
        """
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
        """
        Method that gives the denomicacion
        :param sub_id: Subestacio id
        :return: denominacion
        """
        o = self.connection
        res = ''
        ct_id = o.GiscedataCtsSubestacions.read(sub_id, ['ct_id'])['ct_id'][0]
        if ct_id:
            denom = o.GiscedataCts.read(ct_id, ['descripcio'])['descripcio']
            if denom:
                res = denom
        return res

    def consumer(self):
        """
        Method that generates the csv file
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'data_pm', 'subestacio_id', 'data_baixa',
            'tipus_instalacio_cnmc_id', 'perc_financament', 'tensio',
            self.compare_field
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
                if pos['tipus_instalacio_cnmc_id']:
                    id_ti = pos.get('tipus_instalacio_cnmc_id')[0]
                    codigo_ccuu = O.GiscedataTipusInstallacio.read(
                        id_ti,
                        ['name'])['name']
                else:
                    codigo_ccuu = ''


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
                if pos[self.compare_field]:
                    last_data = pos[self.compare_field]
                    entregada = F4Res4666(**last_data)
                    actual = F4Res4666(
                        o_sub,
                        pos['cini'],
                        denominacio,
                        codigo_ccuu,
                        comunitat,
                        format_f(tensio),
                        format_f(round(100 - int(pos['perc_financament']))),
                        data_pm,
                        fecha_baja,
                        0
                    )
                    if entregada == actual:
                        estado = 0
                    else:
                        estado = 1
                else:
                    if pos['data_pm']:
                        if pos['data_pm'][:4] != str(self.year):
                            estado = '1'
                        else:
                            estado = '2'
                    else:
                        estado = '1'

                output = [
                    o_sub,
                    pos['cini'] or '',
                    denominacio,
                    codigo_ccuu,
                    comunitat,
                    format_f(tensio, 3),
                    format_f(round(100 - int(pos['perc_financament'])), 2),
                    data_pm or '',
                    fecha_baja,
                    estado
                ]

                if self.extended:
                    if 'subestacio_id' in pos:
                        se = O.GiscedataCtsSubestacions.read(
                            pos['subestacio_id'][0],
                            ['id_provincia', 'id_municipi', ' zona_id']
                        )

                        if 'id_municipi' in se:
                            municipi = O.ResMunicipi.read(
                                se['id_municipi'][0], ['name']
                            )
                            output.append(municipi.get('name', ""))
                        else:
                            output.append("")

                        if 'id_provincia' in se:
                            provincia = O.ResCountryState.read(
                                se['id_provincia'][0], ['name']
                            )
                            output.append(provincia.get('name', ""))
                        else:
                            output.append("")
                    else:
                        output.append(["", ""])

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class POS_INT(MultiprocessBased):
    """
    Class that generates the POS/Cel·les(4) of 4666 report
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(POS_INT, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies POS'
        self.report_name = 'CNMC INVENTARI POS'
        self.compare_field = kwargs["compare_field"]
        self.extended = kwargs.get("extended", False)

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        :return: List of ids
        """
        search_params = [('inventari', '=', 'fiabilitat'),
                         ('cini', 'ilike', 'i28_2%')]
        data_pm = '{0}-01-01'.format(self.year + 1)
        search_params += ['|', ('data_pm', '=', False),
                          ('data_pm', '<', data_pm)]
        return self.connection.GiscedataCellesCella.search(
            search_params, 0, 0, False, {'active_test': False})

    def get_comunitat(self, id_ct):
        """
        Gets the comunitat from a subestacion

        :param id_ct: Id of ct
        :return: Comunitat name
        """
        O = self.connection
        comunitat = ''
        cts = O.GiscedataCts.read(id_ct, ['id_municipi'])
        if cts['id_municipi']:
            id_municipi = cts['id_municipi'][0]
        else:
            id_municipi = get_id_municipi_from_company(O)

        if id_municipi:
            # funció per trobar la ccaa desde el municipi
            fun_ccaa = O.ResComunitat_autonoma.get_ccaa_from_municipi
            id_comunitat = fun_ccaa(id_municipi)
            comunitat_vals = O.ResComunitat_autonoma.read(
                id_comunitat[0], ['codi'])
            if comunitat_vals:
                comunitat = comunitat_vals['codi']
        return comunitat

    def get_denominacion(self, ct_id):
        """
        Returns the name of the CT

        :param ct_id:  Name of the ct
        :return: Name of the CT
        """
        o = self.connection
        res = ''

        denom = o.GiscedataCts.read(ct_id, ['name'])['name']
        if denom:
            res = denom
        return res

    def consumer(self):
        """
        Method that generates the csv file
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'subestacio_id', 'tipus_instalacio_cnmc_id',
            'perc_financament', 'tensio', 'data_baixa', 'data_pm',
            self.compare_field, 'installacio'
        ]
        not_found_msg = '**** ERROR: El ct {0} (id:{1}) no està a giscedata_cts_subestacions_posicio.\n'
        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa_limit = '{0}-01-01'.format(self.year)
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                cel = O.GiscedataCellesCella.read(
                    item, fields_to_read)
                identificador = cel["name"]

                data_baixa = ""
                if cel["data_baixa"]:
                    data_baixa = cel["data_baixa"]

                denominacion = ""
                codigo_ccaa = ""
                if cel["subestacio_id"] or cel["installacio"]:
                    ident = int(cel["installacio"].split(",")[1])
                    codigo_ccaa = self.get_comunitat(ident)

                if cel["installacio"]:
                    denominacion = self.get_denominacion(ident) + "-CT"

                codigo_ccuu = ""
                if cel["tipus_instalacio_cnmc_id"]:
                    id_ti = cel["tipus_instalacio_cnmc_id"][0]
                    codigo_ccuu = O.GiscedataTipusInstallacio.read(
                        id_ti, ["name"])["name"]

                tensio = 0.000
                if cel["tensio"]:
                    tensio = float(cel["tensio"][1])/1000.0


                data_pm = ""
                # Calculem any posada en marxa
                if cel["data_pm"]:
                    data_pm = cel["data_pm"]
                    if data_pm:
                        data_pm = datetime.strptime(str(data_pm), "%Y-%m-%d")
                        data_pm = data_pm.strftime("%d/%m/%Y")

                if cel[self.compare_field]:
                    last_data = cel[self.compare_field]
                    entregada = F4Res4666(**last_data)
                    actual = F4Res4666(
                        identificador,
                        cel['cini'],
                        denominacion,
                        codigo_ccuu,
                        codigo_ccaa,
                        format_f(tensio, 3),
                        format_f(round(100 - int(cel['perc_financament']))),
                        data_pm
                    )
                    if entregada == actual:
                        estado = 0
                    else:
                        estado = 1
                else:
                    if cel['data_pm']:
                        if cel['data_pm'][:4] != str(self.year):
                            estado = '1'
                        else:
                            estado = '2'
                    else:
                        estado = '1'

                output = [
                    identificador,
                    cel["cini"] or "",
                    denominacion,
                    codigo_ccuu,
                    codigo_ccaa,
                    format_f(tensio, 3),
                    format_f(round(100 - int(cel['perc_financament'])), 2),
                    data_pm or '',
                    data_baixa,
                    estado
                ]

                if self.extended:
                    if 'subestacio_id' in cel:
                        se = O.GiscedataCtsSubestacions.read(
                            cel['subestacio_id'][0],
                            ['id_provincia', 'id_municipi', ' zona_id']
                        )

                        if 'id_municipi' in se:
                            municipi = O.ResMunicipi.read(
                                se['id_municipi'][0], ['name']
                            )
                            output.append(municipi.get('name', ""))
                        else:
                            output.append("")

                        if 'id_provincia' in se:
                            provincia = O.ResCountryState.read(
                                se['id_provincia'][0], ['name']
                            )
                            output.append(provincia.get('name', ""))
                        else:
                            output.append("")

                    else:
                        output.append(["", ""])

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
