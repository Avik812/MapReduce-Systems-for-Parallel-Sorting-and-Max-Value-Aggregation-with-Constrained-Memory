import multiprocessing as mp
import time, tracemalloc
from utils.helpers import split_into_chunks, measure_memory_mb

def _proc_worker(chunk, shared_val, lock):
    local_max = max(chunk)
    with lock:
        if local_max > shared_val.value:
            shared_val.value = local_max

def max_process(data, workers):
    chunks = split_into_chunks(data, workers)
    shared_val = mp.Value('i', -99999999)
    lock = mp.Lock()

    tracemalloc.start()
    start = time.perf_counter()

    procs = []
    for ch in chunks:
        p = mp.Process(target=_proc_worker, args=(ch, shared_val, lock))
        p.start()
        procs.append(p)
    for p in procs:
        p.join()

    elapsed = time.perf_counter() - start
    mem = measure_memory_mb()
    tracemalloc.stop()

    return shared_val.value, elapsed, mem