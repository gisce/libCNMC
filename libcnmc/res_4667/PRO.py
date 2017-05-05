#!/usr/bin/env python
# -*- coding: utf-8 -*-

import traceback

from libcnmc.core import MultiprocessBased


class PRO(MultiprocessBased):
    """
    Class to generate proyectos of 4667
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        """

        super(PRO, self).__init__(**kwargs)

    def get_sequence(self):
        """
        Generates the sequence of ids to make the report

        :return: List of ids
        :rtype: list
        """

        search_params = []
        return self.connection.GiscedataCnmcProjectes.search(search_params)

    def consumer(self):
        """
        Generates the line of the file
        :return: Line 
        :rtype: str
        """

        O = self.connection
        fields_to_read = [

        ]

        while True:
            try:
                item = self.input_q.get()
                self.progress_q.put(item)

                pro = O.GiscedataCnmcProjectes.read(item, fields_to_read)
                output = [

                ]
                self.output_q.put(output)
            except Exception:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
