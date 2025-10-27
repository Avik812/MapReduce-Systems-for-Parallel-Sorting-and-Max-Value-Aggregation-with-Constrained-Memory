import threading, time, tracemalloc
from utils.helpers import split_into_chunks, measure_memory_mb

def max_threaded(data, workers):
    chunks = split_into_chunks(data, workers)
    global_max = {"val": float("-inf")}
    lock = threading.Lock()

    def worker(chunk):
        local_max = max(chunk)
        with lock:
            if local_max > global_max["val"]:
                global_max["val"] = local_max

    tracemalloc.start()
    start = time.perf_counter()

    threads = []
    for ch in chunks:
        t = threading.Thread(target=worker, args=(ch,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    elapsed = time.perf_counter() - start
    mem = measure_memory_mb()
    tracemalloc.stop()

    return global_max["val"], elapsed, mem