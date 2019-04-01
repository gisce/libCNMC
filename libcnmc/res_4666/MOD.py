#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
from datetime import datetime
import traceback
from libcnmc.core import MultiprocessBased


def get_ti_name(connection, ti_id):
    """
    Get the name of the ti from the id
    
    :param connection: OpenERP conection
    :param ti_id: id of the TI
    :type ti_id: tuple
    :return: Name of the TI
    :rtype: str
    """
    if ti_id:
        model_ti = connection.GiscedataTipusInstallacio
        codigo_ccuu = model_ti.read(ti_id[0], ['name'])['name']
        return codigo_ccuu
    else:
        return ""


class ModCts(MultiprocessBased):
    """
    Class that generates the modification file of CT
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: None
        :rtype: None
        """

        super(ModCts, self).__init__(**kwargs)
        self.explain = kwargs.pop('explain', False)
        self.year = kwargs.pop('year', datetime.now().year - 1)

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consumer
        
        :return: Ids to generate
        :rtype: list
        """

        search_params = [('id_installacio.name', '!=', 'SE')]
        data_pm = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
        search_params += [('propietari', '=', True),
                          '|', ('data_pm', '=', False),
                               ('data_pm', '<', data_pm),
                          '|',
                          '&', ('data_baixa', '>', data_baixa),
                               ('ct_baixa', '=', True),
                          '|',
                               ('data_baixa', '=', False),
                               ('ct_baixa', '=', False)
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataCts.search(
            search_params, 0, 0, False, {'active_test': False})

    def consumer(self):
        """
        Method that generates the csb file
        
        :return: None
        :rtype: None
        """

        O = self.connection

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                fields_to_read = [
                    "4666_entregada_2017", "4771_entregada", "name"
                ]
                ct = O.GiscedataCts.read(item, fields_to_read)
                ti_old = ct["4771_entregada"].get("codigo_tipo_ct", "")
                ti = ct["4666_entregada_2017"].get("codigo_ccuu", "")

                if self.explain:
                    if ti_old:
                        if ti_old == ti:
                            print("[CT] id:{} ti_old:{} == ti:{}".format(ct["id"], ti_old, ti))
                    else:
                        print("[CT] id:{} has no old ti".format(ct["id"]))

                if ti_old and ti_old != ti:
                    output = [
                        ct["name"],
                        "",
                        ti_old,
                        ti
                    ]
                    self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class ModFia(MultiprocessBased):
    """
    Class that generates the modifications of fiabilidad(7)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: None
        :rtype: None
        """

        super(ModFia, self).__init__(**kwargs)
        self.explain = kwargs.pop("explain", False)
        self.year = kwargs.pop('year', datetime.now().year - 1)

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consumer
        
        :return: ids
        :rtype: list
        """

        search_params = [('inventari', '=', 'fiabilitat')]
        data_pm = '{0}-01-01' .format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
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
        search_params += [("cini", "not like", "I28")]
        return self.connection.GiscedataCellesCella.search(
            search_params, 0, 0, False, {'active_test': False})

    def consumer(self):
        """
        Method that generates the csv file
        
        :return: None
        :rtype: None
        """

        O = self.connection
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                fields_to_read = [
                    "4666_entregada_2017", "4771_entregada", "name"
                ]
                cll = O.GiscedataCellesCella.read(item, fields_to_read)

                ti_old = cll["4771_entregada"].get("codigo_tipo_inst", "")
                ti = cll["4666_entregada_2017"].get("codigo_ccuu", "")

                if self.explain:
                    if ti_old:
                        if ti_old == ti:
                            print("[FIA] id:{} ti_old:{} == ti:{}".format(cll["id"], ti_old, ti))
                    else:
                        print("[FIA] id:{} has no old ti".format(cll["id"]))

                if ti_old and ti_old != ti:
                    output = [
                        cll["name"],
                        "",
                        ti_old,
                        ti
                    ]
                    self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class ModLat(MultiprocessBased):
    """
    Class that generates modifications of the LAT(1)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
    
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: None
        :rtype: None
        """

        super(ModLat, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.explain = kwargs.pop("explain", False)

        id_res_like = self.connection.ResConfig.search(
            [('name', '=', 'giscegis_btlike_layer')])
        if id_res_like:
            self.layer = self.connection.ResConfig.read(
                id_res_like, ['value'])[0]['value']

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consumer
       
        :return: List of ids
        """

        search_params = [('propietari', '=', True)]
        obj_lat = self.connection.GiscedataAtLinia
        ids = obj_lat.search(search_params, 0, 0, False, {'active_test': False})

        return ids

    def consumer(self):
        """
        Method that generates the csb file
        
        :return: None
        :rtype: None
        """

        O = self.connection

        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)

        static_search_params = [
            ('propietari', '=', True),
            '|', ('data_pm', '=', False), ('data_pm', '<', data_pm_limit),
            '|', ('data_baixa', '=', False), ('data_baixa', '>', data_baixa)
            ]

        # Revisem que si està de baixa ha de tenir la data informada.
        static_search_params += [
            '|',
            '&', ('active', '=', False), ('data_baixa', '!=', False),
            ('active', '=', True)
        ]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataAtLinia.read(item, ['trams'])

                search_params = [('linia', '=', linia['id'])]
                search_params += static_search_params
                ids = O.GiscedataAtTram.search(
                    search_params, 0, 0, False, {'active_test': False})
                fields_to_read_tram = [
                    "4666_entregada_2017",
                    "4771_entregada",
                    "name"
                ]

                for tram in O.GiscedataAtTram.read(ids, fields_to_read_tram):
                    ti_old = tram["4771_entregada"].get("codigo_tipo_linea", "")
                    ti = tram["4666_entregada_2017"].get("codigo_ccuu", "")

                    if self.explain:
                        if ti_old:
                            if ti_old == ti:
                                print("[LAT] id:{} ti_old:{} == ti:{}".format(tram["id"], ti_old, ti))
                        else:
                            print("[LAT] id:{} has no old ti".format(tram["id"]))

                    if ti_old and ti_old != ti:
                        output = [
                            'A{0}'.format(tram["name"]),
                            "",
                            ti_old,
                            ti
                        ]
                        self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class ModLbt(MultiprocessBased):
    """
    Class that generates the modifications of LBT(2)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
    
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """

        super(ModLbt, self).__init__(**kwargs)
        self.year = kwargs.pop("year", datetime.now().year - 1)
        self.explain = kwargs.pop("explain", False)

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consumer
        
        :return: Ids
        :rtype: list
        """

        data_pm = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
        search_params = []
        search_params += [('propietari', '=', True),
                          '|', ('data_pm', '=', False),
                               ('data_pm', '<', data_pm),
                          '|',
                          '&', ('data_baixa', '>', data_baixa),
                               ('baixa', '=', True),
                          '|',
                               ('data_baixa', '=', False),
                               ('baixa', '=', False)
                          ]
        # Revisem que si està de baixa ha de tenir la data informada.
        search_params += ['|',
                          '&', ('active', '=', False),
                               ('data_baixa', '!=', False),
                          ('active', '=', True)]
        return self.connection.GiscedataBtElement.search(
            search_params, 0, 0, False, {'active_test': False})

    def consumer(self):
        """
        Method that generates the csb file

        :return: None
        :rtype: None
        """

        O = self.connection
        count = 0
        fields_to_read = [
            "4666_entregada_2017", "4771_entregada", "name"
        ]
        while True:
            try:
                count += 1
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataBtElement.read(item, fields_to_read)
                ti_old = linia["4771_entregada"].get("codigo_tipo_linea", "")
                ti = linia["4666_entregada_2017"].get("codigo_ccuu", "")

                if self.explain:
                    if ti_old:
                        if ti_old == ti:
                            print("[LBT] id:{} ti_old:{} == ti:{}".format(linia["id"], ti_old, ti))
                    else:
                        print("[LBT] id:{} has no old ti".format(linia["id"]))

                if ti_old and ti_old != ti:
                    output = [
                        "B{}".format(linia["name"]),
                        "",
                        ti_old,
                        ti
                    ]

                    self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class ModMaq(MultiprocessBased):
    """
    Class that generates the modifications of Maquinas/Transofrmadores(5)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
    
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: None
        :rtype: None
        """
        super(ModMaq, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.explain = kwargs.pop("explai", False)

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consumer
        
        :return: Ids of maquines
        :rtype: list
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

        # search_params_reductor += search_params
        ids_reductor = self.connection.GiscedataTransformadorTrafo.search(
            search_params_reductor, 0, 0, False, {'active_test': False})

        ids_transformadors = self.connection.GiscedataTransformadorTrafo.search(
            search_params_transformadors, 0, 0, False, {'active_test': False})
        return list(set(ids_reductor + ids_transformadors))

    def consumer(self):
        """
        Method that generates each line of the CSV
        
        :return: None
        :rtype: None
        """

        O = self.connection
        fields_to_read = ["4666_entregada_2017", "4771_entregada", "name"]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                trafo = O.GiscedataTransformadorTrafo.read(item, fields_to_read)
                ti_old = trafo["4771_entregada"].get("codigo_tipo_maquina", "")
                ti = trafo["4666_entregada_2017"].get("codigo_ccuu", "")

                if self.explain:
                    if ti_old:
                        if ti_old == ti:
                            print("[MAQ] id:{} ti_old:{} == ti:{}".format(trafo["id"], ti_old, ti))
                    else:
                        print("[MAQ] id:{} has no old ti".format(trafo["id"]))

                if ti_old and ti != ti_old:
                    output = [
                        trafo["name"],
                        "",
                        ti_old,
                        ti
                    ]
                    self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class ModPos(MultiprocessBased):
    """
    Class that generates the modifications of POS/Interruptores(4)
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """

        super(ModPos, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.explain = kwargs.pop("explain", False)

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consumer
        
        :return: Ids
        :rtype: list
        """

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

    def consumer(self):
        """
        Method that generates the csv file

        :return: None
        :rtype: None
        """

        O = self.connection
        fields_to_read = [
            "4666_entregada_2017",
            "4771_entregada",
            "name"
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                pos = O.GiscedataCtsSubestacionsPosicio.read(item, fields_to_read)

                ti_old = pos["4771_entregada"].get("codigo_tipo_ct", "")
                ti = pos["4666_entregada_2017"].get("codigo_ccuu", "")

                if self.explain:
                    if ti_old:
                        if ti_old == ti:
                            print("[POS] id:{} ti_old:{} == ti:{}".format(pos["id"], ti_old, ti))
                    else:
                        print("[POS] id:{} has no old ti".format(pos["id"]))

                if ti_old and ti != ti_old:
                    output = [
                        pos["name"],
                        "",
                        ti_old,
                        ti
                    ]
                    self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
