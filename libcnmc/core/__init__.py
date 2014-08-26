# -*- coding: utf-8 -*-
import csv
import multiprocessing
import sys
from datetime import datetime
try:
    from cStringIO import StringIO
except:
    from StringIO import StringIO

from progressbar import ProgressBar, ETA, Percentage, Bar
from libcnmc.utils import N_PROC


class MultiprocessBased(object):

    def __init__(self, **kwargs):
        self.file_output = kwargs.pop('output', False)
        self.content = None
        self.connection = kwargs.pop('connection')
        self.n_proc = kwargs.pop('n_proc', N_PROC)
        self.content = None
        self.input_q = multiprocessing.JoinableQueue()
        self.output_q = multiprocessing.Queue()
        self.progress_q = multiprocessing.Queue()
        self.quiet = kwargs.pop('quiet', False)
        self.interactive = kwargs.pop('interactive', False)
        self.report_name = ''
        self.base_object = ''

    def get_sequence(self):
        raise NotImplementedError()

    def producer(self, sequence):
        """Posem els items que serviran per fer l'informe.
        """
        for item in sequence:
            self.input_q.put(item)

    def progress(self, total):
        """Rendering del progressbar de l'informe.
        """
        widgets = ['Informe %s: ' % self.report_name,
                   Percentage(), ' ', Bar(), ' ', ETA()]
        pbar = ProgressBar(widgets=widgets, maxval=total).start()
        done = 0
        while True:
            self.progress_q.get()
            done += 1
            pbar.update(done)
            if done >= total:
                pbar.finish()

    def consumer(self):
        raise NotImplementedError()

    def calc(self):
        sequence = []
        sequence += self.get_sequence()
        if not self.quiet or self.interactive:
            sys.stderr.write("S'han trobat %s %s.\n" % (self.base_object, len(sequence)))
            sys.stderr.write("Any %d.\n" % self.year)
            sys.stderr.flush()
        if self.interactive:
            sys.stderr.write("Correcte? ")
            raw_input()
            sys.stderr.flush()
        start = datetime.now()
        processes = [multiprocessing.Process(target=self.consumer)
                     for _ in range(0, N_PROC)]
        if not self.quiet:
            processes += [
                multiprocessing.Process(
                    target=self.progress, args=(len(sequence),)
                )
            ]
        for proc in processes:
            proc.daemon = True
            proc.start()
            if not self.quiet:
                sys.stderr.write("^Starting process PID (%s): %s\n" %
                                 (proc.name, proc.pid))
        sys.stderr.flush()
        self.producer(sequence)
        self.input_q.join()
        if not self.quiet:
            sys.stderr.write("Time Elapsed: %s\n" % (datetime.now() - start))
            sys.stderr.flush()
        if self.file_output:
            self.content = open(self.file_output, 'wb')
        else:
            self.content = StringIO()
        fitxer = csv.writer(self.content, delimiter=';', lineterminator='\n')
        while not self.output_q.empty():
            msg = self.output_q.get()
            msg = map(lambda x: type(x)==unicode and x.encode('utf-8') or x, msg)
            fitxer.writerow(msg)
        self.content.close()
