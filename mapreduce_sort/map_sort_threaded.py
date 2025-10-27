import threading, time, tracemalloc
from utils.helpers import split_into_chunks, measure_memory_mb

def map_sort_threaded(data, workers):
    chunks = split_into_chunks(data, workers)
    sorted_chunks = [None] * len(chunks)

    def worker(i, chunk):
        sorted_chunks[i] = sorted(chunk)

    tracemalloc.start()
    start = time.perf_counter()

    threads = []
    for i, ch in enumerate(chunks):
        t = threading.Thread(target=worker, args=(i, ch))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    elapsed = time.perf_counter() - start
    mem = measure_memory_mb()
    tracemalloc.stop()

    return sorted_chunks, elapsed, mem