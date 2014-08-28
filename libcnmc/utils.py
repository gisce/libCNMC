import os
import multiprocessing


N_PROC = int(os.getenv('N_PROC', multiprocessing.cpu_count() + 1))


CODIS_TARIFA = {'2.0A': 416, '2.0DHA': 417, '2.1A': 418, '2.1DHA': 419,
                '2.0DHS': 426, '2.1DHS': 427, '3.0A': 403, '3.1A': 404,
                '3.1A LB': 404,  '6.1': 405}


CODIS_ZONA = {'RURALDISPERSA': 'RD', 'RURALCONCENTRADA': 'RC',
              'SEMIURBANA': 'SU', 'URBANA': 'U'}


CINI_TELEGESTIO = 'I290000'
