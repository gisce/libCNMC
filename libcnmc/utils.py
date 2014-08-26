import os
import multiprocessing


N_PROC = int(os.getenv('N_PROC', multiprocessing.cpu_count() + 1))


