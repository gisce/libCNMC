#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
INVENTARI DE CNMC Condensadors
"""
from __future__ import absolute_import
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased
from libcnmc.utils import format_f, get_id_municipi_from_company
from libcnmc.models import F8Res4131


class MOD_CON(MultiprocessBased):
    """
    Class that generates the Maquinas/Condensadores(5) file of the 4131
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """

        super(MOD_CON, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        
        :return: List of ids
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

        ids_condensadors = self.connection.GiscedataCondensadors.search(
            search_params, 0, 0, False, {'active_test': False})
        return ids_condensadors

    def consumer(self):
        """
        Method that generates the csv file
        
        :return: List of arrays
        """

        O = self.connection

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)



                output = [
                    # id bd
                    # id actual
                    # ti antic
                    # ti actual
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class MOD_CTS(MultiprocessBased):
    """
    Class that generates the CT file of the 4131
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: None
        :rtype: None
        """

        super(MOD_CTS, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies CTS'
        self.report_name = 'CNMC INVENTARI CTS'
        self.compare_field = kwargs["compare_field"]

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
                          '|', ('data_baixa', '>', data_baixa),
                               ('data_baixa', '=', False)
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
        
        :return: List of arrays
        """

        O = self.connection

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                output = [
                    # id db
                    # id act
                    # ti
                    # ti act
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class MOD_DES(MultiprocessBased):
    """
    Class that generates the Despachos(6) file of the 4131
    """

    def __init__(self, **kwargs):
        """
        Class constructor
    
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(MOD_DES, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies DES'
        self.report_name = 'CNMC INVENTARI DES'
        self.compare_field = kwargs["compare_field"]

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
    
        :return: List of ids
        """
        data_limit = '01-01-{}'.format(self.year+1)
        search_params = [('data_apm', '<=', data_limit)]
        return self.connection.GiscedataDespatx.search(search_params)

    def consumer(self):
        """
        Method that generates the csb file
    
        :return: List of arrays
        """
        O = self.connection
        fields_to_read = [
            'name', 'cini', 'denominacio', 'any_ps', 'vai', 'data_apm',
            self.compare_field
        ]
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                despatx = O.GiscedataDespatx.read(item, fields_to_read)

                output = [
                    # id bd
                    # id act
                    # ti
                    # ti act
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class MOD_FIA(MultiprocessBased):
    """
    Class that generates the fiabilidad(7) file of the 4131
    """
    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: None
        :rtype: None
        """

        super(MOD_FIA, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies FIA'
        self.report_name = 'CNMC INVENTARI FIA'
        self.compare_filed = kwargs["compare_field"]

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consummer
        
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
        Method that generates the csb file
        
        :return: None
        :rtype: None
        """

        O = self.connection
        fields_to_read = []
        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                cll = O.GiscedataCellesCella.read(item, fields_to_read)

                output = [
                    # id bd
                    # id act
                    # ti
                    # ti act
                ]
                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class MOD_LAT(MultiprocessBased):
    """
    Class that generates the LAT(1) file of  4131
    """

    def __init__(self, **kwargs):
        """
        Class constructor
    
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: None
        :rtype: None
        """

        super(MOD_LAT, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies AT'
        self.report_name = 'CNMC INVENTARI AT'
        self.layer = 'LBT\_%'
        self.embarrats = kwargs.pop('embarrats', False)
        self.compare_field = kwargs["compare_field"]
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
        id_lat_emb = []
        if self.embarrats:
            id_lat_emb = obj_lat.search(
                [('name', '=', '1')], 0, 0, False, {'active_test': False})
        return ids + id_lat_emb

    def consumer(self):
        """
        Method that generates the csb file
        
        :return: None
        :rtype: None
        """

        O = self.connection
        fields_to_read = [
            'baixa', 'data_pm', 'data_industria', 'coeficient', 'cini',
            'propietari', 'tensio_max_disseny', 'name', 'origen', 'final',
            'perc_financament', 'circuits', 'longitud_cad', 'cable',
            'tipus_instalacio_cnmc_id', 'data_baixa', self.compare_field,
            'baixa', 'data_baixa'
        ]
        data_pm_limit = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)

        static_search_params = [
            ('propietari', '=', True),
            '|', ('data_pm', '=', False), ('data_pm', '<', data_pm_limit),
            '|', ('data_baixa', '=', False), ('data_baixa', '>', data_baixa)
            ]

        # print 'static_search_params:{}'.format(static_search_params)
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

                linia = O.GiscedataAtLinia.read(
                    item, ['trams', 'tensio', 'municipi', 'propietari']
                )

                search_params = [('linia', '=', linia['id'])]
                search_params += static_search_params
                ids = O.GiscedataAtTram.search(
                    search_params, 0, 0, False, {'active_test': False})
                id_desconegut = O.GiscedataAtCables.search(
                    [('name', '=', 'DESCONEGUT')])

                if not id_desconegut:
                    id_desconegut = O.GiscedataAtCables.search(
                        [('name', '=', 'DESCONOCIDO')])[0]
                for tram in O.GiscedataAtTram.read(ids, fields_to_read):
                    # Comprovar el tipus del cable
                    pass

                    output = [
                        # id bd
                        # id act
                        # ti
                        # ti act
                    ]

                    self.output_q.put(output)

            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class MOD_LBT(MultiprocessBased):
    """
    Class that generates the LBT(2) file of the 4131
    """

    def __init__(self, **kwargs):
        """
        Class constructor
    
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """

        super(MOD_LBT, self).__init__(**kwargs)
        self.year = kwargs.pop("year", datetime.now().year - 1)
        self.codi_r1 = kwargs.pop("codi_r1")
        self.base_object = "Línies BT"
        self.report_name = "CNMC INVENTARI BT"
        self.embarrats = kwargs.pop("embarrats", False)
        self.compare_field = kwargs["compare_field"]

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consumer
        
        :return: Ids
        :rtype: list
        """

        data_pm = '{0}-01-01'.format(self.year + 1)
        data_baixa = '{0}-01-01'.format(self.year)
        search_params = []
        if not self.embarrats:
            search_params += [('cable.tipus.codi', '!=', 'E')]
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
            'name', 'municipi', 'data_pm', 'ct', 'coeficient', 'cini',
            'perc_financament', 'longitud_cad', 'cable', 'voltatge',
            'data_alta', 'propietari', 'tipus_instalacio_cnmc_id', 'baixa',
            'data_baixa', self.compare_field
        ]
        while True:
            try:
                count += 1
                item = self.input_q.get()
                self.progress_q.put(item)

                linia = O.GiscedataBtElement.read(item, fields_to_read)

                output = [
                    # id bd
                    # id act
                    # ti
                    # ti act
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class MOD_MAQ(MultiprocessBased):
    """
    Class that generates the Maquinas/Transofrmadores(5) file of the 4131
    """

    def __init__(self, **kwargs):
        """
        Class constructor
    
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: None
        :rtype: None
        """
        super(MOD_MAQ, self).__init__(**kwargs)
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
        self.report_name = 'CNMC INVENTARI MAQ'

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
            '|', ('data_baixa', '>', data_baixa),
            ('data_baixa', '=', False)
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
        return list(set(ids_reductor + ids_transformadors))

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

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                trafo = O.GiscedataTransformadorTrafo.read(item, fields_to_read)

                output = [
                    # id bd
                    # id act
                    # ti
                    # ti act
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class MOD_POS(MultiprocessBased):
    """
    Class that generates the POS/Interruptores(4) of 4131 report
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """

        super(MOD_POS, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Línies POS'
        self.report_name = 'CNMC INVENTARI POS'
        self.compare_field = kwargs["compare_field"]

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consumer
        
        :return: Idst
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
            'name', 'cini', 'data_pm', 'subestacio_id', 'data_baixa',
            'tipus_instalacio_cnmc_id', 'perc_financament', 'tensio',
            self.compare_field
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)
                pos = O.GiscedataCtsSubestacionsPosicio.read(item, fields_to_read)


                output = [
                    # id bd
                    # id act
                    # ti
                    # ti act
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()


class MOD_SUB(MultiprocessBased):
    """
    Class that generates the SUB(3) report of the 4131
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: year(generation year), codi_r1 R1 code
        :return: CT
        """
        super(MOD_SUB, self).__init__(**kwargs)
        self.year = kwargs.pop('year', datetime.now().year - 1)
        self.codi_r1 = kwargs.pop('codi_r1')
        self.base_object = 'Subestacions'
        self.report_name = 'CNMC INVENTARI SUB'
        self.compare_field = kwargs["compare_field"]

    def get_sequence(self):
        """
        Method that generates a list of ids to pass to the consumer
        
        :return: Ids to generate
        :rtype: list
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
        
        :return: None
        :rtype: None
        """

        O = self.connection
        fields_to_read = [
            'name', 'data_industria', 'data_pm', 'id_municipi', 'cini',
            'descripcio', 'perc_financament', 'data_baixa', 'posicions',
            'cnmc_tipo_instalacion', self.compare_field
        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                sub = O.GiscedataCtsSubestacions.read(item, fields_to_read)

                output = [
                    # id bd
                    # id act
                    # ti
                    # ti act
                ]

                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
