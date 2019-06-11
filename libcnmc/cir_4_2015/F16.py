# -*- coding: utf-8 -*-
from datetime import datetime
import traceback

from libcnmc.utils import get_ine, format_f, convert_srid, get_srid, get_total_elements
from libcnmc.core import MultiprocessBased


class F16(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F16, self).__init__(**kwargs)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F16 - Condensadors'
        self.base_object = 'Condensadors'

    def get_sequence(self):
        data_pm = '%s-01-01' % (self.year + 1)
        data_baixa = '%s-12-31' % self.year
        search_params = ['|', ('data_pm', '=', False),
                         ('data_pm', '<', data_pm),
                         '|', ('data_baixa', '>', data_baixa),
                         ('data_baixa', '=', False)]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        ids = self.connection.GiscedataCondensadors.search(
            search_params, 0, 0, False, {'active_test': False}
        )
        return get_total_elements(self.connection, "giscedata.condensadors", ids)


    def get_node_vertex(self, cond_name):
        """
        Returns the node and vertex of the condensador
        :param cond_name:
        :type cond_name: str
        :return: node,vertex
        :rtype: (str, str)
        """

        O = self.connection
        ident = O.GiscegisElementsbt.search([('codi', '=', cond_name)])
        if not ident:
            ident = O.GiscegisElementsat.search([('codi', '=', cond_name)])
            if ident:
                data = O.GiscegisElementsat.read(ident[0], ["node", "vertex"])
        else:
            data = O.GiscegisElementsbt.read(ident[0], ["node", "vertex"])

        if not ident:
            node = ''
            vertex = ''
        else:
            node = data["node"][1]
            x, y = data["vertex"][1].split(",")
            vertex = (round(float(x), 3), round(float(y), 3))

        return node, vertex

    def get_ine(self, municipi_id):
        O = self.connection
        muni = O.ResMunicipi.read(municipi_id, ['ine', 'dc'])
        return get_ine(O, muni['ine'])

    def get_dades_ct(self, ct_id):
        O = self.connection
        ct = O.GiscedataCts.read(ct_id, ['id_municipi', 'propietari'])
        return ct

    def get_tensio(self, tensio_id):
        O = self.connection
        tensio = O.GiscedataTensionsTensio.read(tensio_id, ['tensio'])
        return tensio['tensio']

    def consumer(self):
        o_codi_r1 = 'R1-%s' % self.codi_r1[-3:]
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'ct_id', 'tensio_id', 'potencia_instalada'
        ]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                condensador = O.GiscedataCondensadors.read(item, fields_to_read)
                o_cond = condensador['name']
                o_cini = condensador.get('cini', '')
                o_potencia = condensador['potencia_instalada']

                o_node, posicion = self.get_node_vertex(condensador['name'])
                o_node = o_node.replace('*', '')

                ct = self.get_dades_ct(condensador['ct_id'][0])
                o_propietari = int(ct['propietari'])

                o_ine_muni, o_ine_prov = '', ''
                if ct['id_municipi']:
                    o_ine_prov, o_ine_muni = self.get_ine(ct['id_municipi'][0])
                o_tensio = format_f(
                    float(self.get_tensio(condensador['tensio_id'][0])) / 1000.0,
                    decimals=3
                )

                o_any = self.year
                z = ''
                res_srid = ['', '']
                if posicion:
                    res_srid = convert_srid(
                        self.codi_r1, get_srid(O), posicion)
                self.output_q.put([
                    o_node,                             # NUDO
                    o_cond,                             # CONDENSADOR
                    o_cini,                             # CINI
                    format_f(res_srid[0], decimals=3),  # X
                    format_f(res_srid[1], decimals=3),  # Y
                    z,                                  # Z
                    o_ine_muni,                         # MUNICIPIO
                    o_ine_prov,                         # PROVINCIA
                    o_tensio,                           # NIVEL TENSION
                    format_f(o_potencia, decimals=3),   # POTENCIA INSTALADA
                    o_codi_r1,                          # CODIGO DISTRIBUIDORA
                    o_propietari,                       # PROPIETARIO
                    o_any                               # AÑO INFORMACION
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
