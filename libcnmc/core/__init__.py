# -*- coding: utf-8 -*-
import csv
import multiprocessing
import os
import sys
from datetime import datetime
from io import BytesIO
import traceback

try:
    from raven import Client
except:
    Client = None

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None
from progressbar import ProgressBar, ETA, Percentage, Bar
from libcnmc.utils import N_PROC
from libcnmc import VERSION


class SentryCompatRaven(object):

    def __init__(self, client, client_type):
        self.client = client
        self.client_type = client_type

    def captureException(self):
        if self.client_type == 'raven':
            self.client.captureException()
        else:
            self.client.capture_exception()

    def captureMessage(self, message, level=None, scope=None, **scope_args):
        if self.client_type == 'raven':
            self.client.captureMessage(message, level)
        else:
            self.client.capture_message(message, level, scope, **scope_args)



class MultiprocessBased(object):
    """
    Multiprocess class to generate the files in multiple process
    """

    def __init__(self, **kwargs):
        """
        Class constructor
        :param kwargs: 
        :type kwargs:dict 
        """

        self.file_output = kwargs.pop('output', False)
        self.file_modificaciones = kwargs.pop('modificaciones', False)
        self.connection = kwargs.pop('connection')
        self.num_proc = max(1, kwargs.pop('num_proc', N_PROC))
        self.content = None
        self.input_q = multiprocessing.JoinableQueue()
        self.output_q = multiprocessing.JoinableQueue()
        self.output_m = multiprocessing.JoinableQueue()
        self.progress_q = multiprocessing.Queue()
        self.quiet = kwargs.pop('quiet', False)
        self.pipe = os.environ.get('CNMC_PIPE')
        self.interactive = kwargs.pop('interactive', False)
        self.report_name = ''
        self.base_object = ''
        self.file_header = []
        if self.pipe:
            self.quiet = True
        if 'SENTRY_DSN' in os.environ and Client:
            try:
                raven = Client()
                raven.tags_context({'version': VERSION})
                self.raven = SentryCompatRaven(raven, 'raven')
            except ValueError:
                pass
        else:
            self.raven = None

        if 'SENTRY_DSN' in os.environ and sentry_sdk:
            try:
                sentry_sdk.init(
                    dsn=os.environ['SENTRY_DSN'],
                    release=VERSION
                )
                self.raven = SentryCompatRaven(sentry_sdk, 'sentry_sdk')
            except ValueError:
                pass
        self.content = ''

    def get_sequence(self):
        """
        Generates a list of ids to pass to consumer
                
        :return: List of ids
        :rtype: list
        """

        raise NotImplementedError()

    def producer(self, sequence):
        """
        Posem els items que serviran per fer l'informe.
        :param sequence: 
        :return: 
        """

        for item in sequence:
            self.input_q.put(item)

    def progress(self, total):
        """
        Rendering del progressbar de l'informe.
        
        :param total: 
        :return: 
        """

        widgets = ['Informe %s: ' % self.report_name,
                   Percentage(), ' ', Bar(), ' ', ETA()]
        if total:
            if self.pipe:
                from libcnmc.utils import PipeProgressBar
                pbar = PipeProgressBar(maxval=total).start()
            else:
                pbar = ProgressBar(widgets=widgets, maxval=total).start()
            done = 0
            while done < total:
                self.progress_q.get()
                done += 1
                pbar.update(done)
            pbar.finish()

    def writer(self):
        """
        Writes the data on the output file
        
        :return: None
        :rtype: None
        """

        if self.file_output:
            fio = open(self.file_output, 'wb')
        else:
            fio = BytesIO()
        fitxer = csv.writer(fio, delimiter=';', lineterminator='\n')
        if self.file_header:
            fitxer.writerow(self.file_header)
        while True:
            try:
                item = self.output_q.get()
                if item == 'STOP':
                    self.output_q.task_done()
                    break
                msg = map(lambda x: type(x)==unicode and x.encode('utf-8') or x, item)
                fitxer.writerow(msg)
                self.output_q.task_done()
            except:
                self.output_q.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()

        if not self.file_output:
            self.content = fio.getvalue()
        fio.close()

    def writer_mod(self):
        """Worker to write modification
        """
        if self.file_modificaciones:
            fio_mod = open(self.file_modificaciones, 'wb')
        else:
            fio_mod = BytesIO()

        while True:
            try:
                item = self.output_m.get()
                if item == 'STOP':
                    self.output_m.task_done()
                    break
                fio_mod.writelines(item + "\n")
                self.output_m.task_done()
            except:
                self.output_m.task_done()
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
        fio_mod.close()

    def consumer(self):
        """
        Generates the data for each file
        
        :return: None
        :rtype: None
        """

        raise NotImplementedError()

    def execute(self):
        """
        Alias for calc
        
        :return: None
        :rtype: None
        """

        self.calc()

    def calc(self):
        sequence = []
        sequence += self.get_sequence()
        if not self.quiet or self.interactive:
            sys.stderr.write("S'han trobat %s %s.\n" % (self.base_object, len(sequence)))
            if self.year:
                sys.stderr.write("Any %d.\n" % self.year)
            sys.stderr.flush()
        if self.interactive:
            sys.stderr.write("Correcte? ")
            input()
            sys.stderr.flush()
        start = datetime.now()
        processes = [multiprocessing.Process(target=self.consumer)
                     for _ in range(0, self.num_proc)]
        if not self.quiet or self.pipe:
            processes += [
                multiprocessing.Process(
                    target=self.progress, args=(len(sequence),)
                )
            ]
        processes.append(multiprocessing.Process(target=self.writer))
        processes.append(multiprocessing.Process(target=self.writer_mod))
        self.producer(sequence)
        for proc in processes:
            proc.daemon = True
            proc.start()
            if not self.quiet:
                sys.stderr.write("^Starting process PID (%s): %s\n" %
                                 (proc.name, proc.pid))
        sys.stderr.flush()
        self.input_q.join()
        self.output_q.put('STOP')
        if not self.quiet:
            sys.stderr.write("Time Elapsed: %s\n" % (datetime.now() - start))
            sys.stderr.flush()

        self.output_q.join()
        self.output_m.put('STOP')
        self.output_m.join()

        self.output_q.close()
        self.input_q.close()
        self.output_m.close()


class StopMultiprocessBased(MultiprocessBased):
    def producer(self, sequence):
        super(StopMultiprocessBased, self).producer(sequence)
        for _ in range(0, self.num_proc):
            self.input_q.put('STOP')


class UpdateFile(StopMultiprocessBased):
    def __init__(self, **kwargs):
        super(UpdateFile, self).__init__(**kwargs)
        self.file_input = kwargs.pop('file_input')
        self.file_output = '{}.err'.format(self.file_input)
        self.header = []
        self.search_keys = []
        self.year = False

    def get_sequence(self):
        energies_file = open(self.file_input)
        sequence = energies_file.readlines()
        energies_file.close()
        return sequence

    def search_and_update(self, vals):
        search_params = []
        for header_key, bbdd_key in self.search_keys:
            value = vals.pop(header_key)
            search_params += [(bbdd_key, '=', value)]
        ids = self.object.search(search_params)
        if ids:
            self.object.write(ids, vals)

    def build_vals(self, values):
        vals = {}
        for val in zip(self.header, values):
            vals[val[0]] = val[1]
        return vals

    def consumer(self):
        while True:
            try:
                item = self.input_q.get()
                if item == 'STOP':
                    self.input_q.task_done()
                    break
                self.progress_q.put(item)
                values = item.rstrip().split(';')
                vals = self.build_vals(values)
                self.search_and_update(vals)
                self.input_q.task_done()
            except:
                traceback.print_exc()
                if self.raven:
                    self.raven.captureException()
                self.output_q.put([item.strip()])
                self.input_q.task_done()


class UpdateCNMCStats(UpdateFile):
    def __init__(self, **kwargs):
        super(UpdateCNMCStats, self).__init__(**kwargs)
        self.header = [
            'cups', 'cne_anual_activa', 'cne_anual_reactiva',
            'cnmc_potencia_facturada', 'cnmc_numero_lectures',
            'cne_anual_activa_generada', 'cne_anual_reactiva_generada',
            'cnmc_energia_autoconsumida', 'cnmc_energia_excedentaria',
            'cnmc_factures_total', 'cnmc_factures_estimades',
        ]
        self.search_keys = [('cups', 'name')]
        self.object = self.connection.GiscedataCupsPs


class UpdateCINISComptador(UpdateFile):
    def __init__(self, **kwargs):
        super(UpdateCINISComptador, self).__init__(**kwargs)
        self.header = [
            'numero_comtador', 'cini'
        ]
        self.search_keys = [('numero_comtador', 'name')]
        self.object = self.connection.GiscedataLecturesComptador
