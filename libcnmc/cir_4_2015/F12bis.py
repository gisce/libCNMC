# -*- coding: utf-8 -*-
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased


class F12bis(MultiprocessBased):
    def __init__(self, **kwargs):
        super(F12bis, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.report_name = 'F12 Bis - Cel·les'
        self.base_object = 'Cel·les'
        self.fiabilitat = kwargs.get("fiabilitat")
        self.doslmesp = kwargs.get("doslmesp")

    def get_sequence(self):
        """
        Generates a list of elements of celles to be passed to the consumer

        :return: None
        """

        search_params = [
            ("installacio", "like", "giscedata.cts"),
            ("tipus_element.codi", "!=", "FUS_AT")
        ]

        if self.fiabilitat and self.doslmesp:
            search_params.append(("inventari", "in", ('fiabilitat', 'l2+p')))
        elif self.fiabilitat:
            search_params.append(("inventari", "=", "fiabilitat"))
        elif self.doslmesp:
            search_params.append(("inventari", "=", 'l2+p'))
        else:
            search_params.append(
                ("inventari", "not in", ('fiabilitat', 'l2+p'))
            )

        data_pm = '{}-01-01'.format(self.year + 1)
        data_baixa = '{}-12-31'.format(self.year)
        search_params += ['|', ('data_pm', '=', False),
                          ('data_pm', '<', data_pm),
                          '|', ('data_baixa', '>', data_baixa),
                               ('data_baixa', '=', False)
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataCellesCella.search(
            search_params, 0, 0, False, {'active_test': False})

    def obtenir_ct(self, ct):
        i = 0
        res = 'a'
        es_ct = False
        if ct:
            while i < len(ct):
                if str(ct[i]) == "'" and not es_ct:
                    es_ct = True
                elif str(ct[i]) == "'" and es_ct:
                    es_ct = False
                if es_ct and str(ct[i]) != "'":
                    res += str(ct[i])
                i += 1
        return res

    def get_codi_ct(self, ct_id):
        o = self.connection
        ct = o.GiscedataCts.read(ct_id, ['name'])
        res = ''
        if ct:
            res = ct['name']
        return res

    def get_codi_maquina(self, ct_id):
        o = self.connection
        trafos = o.GiscedataCts.read(
            ct_id, ['transformadors'])['transformadors']
        trafo_id = o.GiscedataTransformadorTrafo.search(
            [('id', 'in', trafos), ('id_estat.codi', '=', 1)], 0, 1)
        res = ''
        if trafo_id:
            codi_maquina = o.GiscedataTransformadorTrafo.read(trafo_id[0],
                                                              ['name'])
            res = codi_maquina['name']
        return res

    def consumer(self):
        o = self.connection
        fields_to_read = [
            'installacio', 'name', 'propietari', 'data_pm', 'cini'
        ]
        while True:
            try:
                # generar linies
                item = self.input_q.get()
                self.progress_q.put(item)
                celles = o.GiscedataCellesCella.read(
                    item, fields_to_read
                )
                o_ct_id = int(celles['installacio'].split(',')[1])
                o_ct = self.get_codi_ct(o_ct_id)
                o_maquina = self.get_codi_maquina(o_ct_id)
                o_posicio = celles['name']
                o_cini = celles['cini'] or ''
                o_propietari = int(celles['propietari'])
                o_data = ''
                if celles['data_pm']:
                    o_data = datetime.strptime(celles['data_pm'], "%Y-%m-%d")
                    o_data = int(o_data.year)
                if o_maquina == '':
                    o_maquina = 999999
                o_any = self.year

                self.output_q.put([
                    o_ct,           # CT
                    o_maquina,      # MAQUINA
                    o_posicio,      # POSICION
                    o_cini,         # CINI
                    o_propietari,   # PROPIEDAD
                    o_data,         # FECHA PUESTA EN SERVICIO
                    o_any           # AÑO INFORMACION
                ])
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
