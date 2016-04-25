# -*- coding: utf-8 -*-
from datetime import datetime
import traceback

from libcnmc.core import MultiprocessBased


class projectes(MultiprocessBased):

    def __init__(self, **kwargs):
        super(projectes, self).__init__(**kwargs)
        self.complet = (kwargs.pop('complet') == '1')
        self.year = kwargs.pop('year')

    def get_sequence(self):
        if self.complet:
            search_params = [('anyo', 'in', [2015, 2016, 2017])]
        else:
            search_params = [('anyo', '=', 2015)]
        return self.connection.GiscedataCnmcPla_inversio.search(
            search_params)

    def default_values(self, value, default):
        if not value:
            return default
        return value

    def consumer(self):
        O = self.connection
        while True:
            try:
                item = self.input_q.get()
                # cada item es un pla d'inversio
                self.progress_q.put(item)
                print "item!: {0}".format(item)
                if item:
                    pla_inversio_id = item
                    ids_anys = self.connection.GiscedataCnmcResum_any.search(
                        [('pla_inversio', '=', pla_inversio_id)], 0, 0, 'anyo'
                    )
                    print "IDS ANYS: {0}".format(ids_anys)
                    for elem in ids_anys:
                        # iterem per cada any del projecte
                        projectes = O.GiscedataCnmcResum_any.read(
                            elem, ['projectes']
                        )['projectes']
                        print "projectes: {0}".format(projectes)
                        for projecte in projectes:
                            p = O.GiscedataCnmcProjectes.read(projecte)
                            # obtenim i printem els projectes de l'any actual
                            if self.complet:
                                pass
                            else:
                                pass
                            self.output_q.put([
                                "{0}".format(p)
                            ])
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
            finally:
                self.input_q.task_done()
