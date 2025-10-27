from multiprocessing import Pool
import time, tracemalloc
from utils.helpers import split_into_chunks, measure_memory_mb

def _worker_sort(chunk):
    return sorted(chunk)

def map_sort_process(data, workers):
    chunks = split_into_chunks(data, workers)

    tracemalloc.start()
    start = time.perf_counter()

    with Pool(processes=workers) as pool:
        sorted_chunks = pool.map(_worker_sort, chunks)

    elapsed = time.perf_counter() - start
    mem = measure_memory_mb()
    tracemalloc.stop()

    return sorted_chunks, elapsed, mem