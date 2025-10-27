#!/usr/bin/env python3
"""
Runnable MapReduce-style demo:
 - Parallel Sorting (threads vs processes)
 - Max-Value Aggregation with constrained shared memory (threads vs processes)

This script runs all experiments automatically (no CLI).
"""

import time
import random
import heapq
import tracemalloc
import multiprocessing as mp
import threading
from typing import List

# ------------------- Utilities -------------------
def chunks_of(data: List[int], k: int):
    n = len(data)
    base = n // k
    rem = n % k
    idx = 0
    for i in range(k):
        sz = base + (1 if i < rem else 0)
        yield data[idx: idx + sz]
        idx += sz

def measure_memory():
    current, _ = tracemalloc.get_traced_memory()
    return current / (1024 * 1024)

def is_sorted(arr: List[int]) -> bool:
    return all(arr[i] <= arr[i+1] for i in range(len(arr)-1))

# ------------------- Sorting mappers/reducers -------------------
def map_sort_chunk(chunk: List[int]) -> List[int]:
    # in-place sort; returns the chunk (sorted)
    chunk.sort()
    return chunk

def reduce_merge_sorted(chunks: List[List[int]]) -> List[int]:
    return list(heapq.merge(*chunks))

# ------------------- Threaded sorting -------------------
def sort_threads(data: List[int], workers: int):
    chunks = list(chunks_of(data, workers))
    sorted_chunks = [None] * workers
    threads = []

    def thread_worker(i, ch):
        sorted_chunks[i] = map_sort_chunk(ch)

    tracemalloc.start()
    start = time.perf_counter()
    for i, ch in enumerate(chunks):
        t = threading.Thread(target=thread_worker, args=(i, ch))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    result = reduce_merge_sorted(sorted_chunks)
    elapsed = time.perf_counter() - start
    heap_mb = measure_memory()
    tracemalloc.stop()
    return result, elapsed, heap_mb

# ------------------- Process sorting -------------------
# map_sort_chunk is top-level so Pool.map can use it
def sort_processes(data: List[int], workers: int):
    chunks = list(chunks_of(data, workers))
    tracemalloc.start()
    start = time.perf_counter()
    with mp.Pool(processes=workers) as pool:
        sorted_chunks = pool.map(map_sort_chunk, chunks)
        result = reduce_merge_sorted(sorted_chunks)
    elapsed = time.perf_counter() - start
    heap_mb = measure_memory()
    tracemalloc.stop()
    return result, elapsed, heap_mb

# ------------------- Max aggregation (threads) -------------------
def max_threads(data: List[int], workers: int):
    chunks = list(chunks_of(data, workers))
    shared = {"max": float("-inf")}
    lock = threading.Lock()

    def thread_worker(ch):
        local_max = max(ch) if ch else float("-inf")
        with lock:
            if local_max > shared["max"]:
                shared["max"] = local_max

    threads = []
    tracemalloc.start()
    start = time.perf_counter()
    for ch in chunks:
        t = threading.Thread(target=thread_worker, args=(ch,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - start
    heap_mb = measure_memory()
    tracemalloc.stop()
    return shared["max"], elapsed, heap_mb

# ------------------- Max aggregation (processes) -------------------
# Worker function must be top-level so it can be spawned/pickled
def process_worker_update(ch: List[int], shared_value, lock):
    local_max = max(ch) if ch else -10**18
    # Acquire lock and update shared_value (inherited by child)
    with lock:
        if local_max > shared_value.value:
            shared_value.value = int(local_max)

def max_processes(data: List[int], workers: int):
    chunks = list(chunks_of(data, workers))

    # Create synchronized objects in parent so children inherit them (spawn/fork-safe)
    shared_value = mp.Value('l', -10**18)  # signed long
    lock = mp.Lock()

    processes = []
    tracemalloc.start()
    start = time.perf_counter()
    for ch in chunks:
        p = mp.Process(target=process_worker_update, args=(ch, shared_value, lock))
        p.start()
        processes.append(p)
    for p in processes:
        p.join()
    elapsed = time.perf_counter() - start
    heap_mb = measure_memory()
    tracemalloc.stop()
    return shared_value.value, elapsed, heap_mb

# ------------------- Experiment Runner -------------------
def run_experiments():
    sizes = [32, 131072]
    worker_counts = [1, 2, 4, 8]
    random.seed(42)

    print("=== MapReduce Parallel Sorting ===")
    for n in sizes:
        data = [random.randint(-10**6, 10**6) for _ in range(n)]
        print(f"\nInput size: {n}")
        print(f"{'Workers':<8} {'Mode':<12} {'Time(s)':<12} {'Heap(MB)':<10} {'Correct':<8}")
        print("-"*60)
        for w in worker_counts:
            # Threads
            res_t, t_t, mem_t = sort_threads(list(data), w)
            ok_t = is_sorted(res_t) and len(res_t) == len(data)
            print(f"{w:<8} {'Threads':<12} {t_t:<12.6f} {mem_t:<10.3f} {ok_t!s:<8}")
            # Processes
            res_p, t_p, mem_p = sort_processes(list(data), w)
            ok_p = is_sorted(res_p) and len(res_p) == len(data)
            print(f"{w:<8} {'Processes':<12} {t_p:<12.6f} {mem_p:<10.3f} {ok_p!s:<8}")

    print("\n=== Max-Value Aggregation ===")
    for n in sizes:
        data = [random.randint(-10**6, 10**6) for _ in range(n)]
        true_max = max(data) if data else None
        print(f"\nInput size: {n}")
        print(f"{'Workers':<8} {'Mode':<12} {'Time(s)':<12} {'Heap(MB)':<10} {'Correct':<8}")
        print("-"*60)
        for w in worker_counts:
            val_t, t_t, mem_t = max_threads(list(data), w)
            ok_t = (val_t == true_max)
            print(f"{w:<8} {'Threads':<12} {t_t:<12.6f} {mem_t:<10.3f} {ok_t!s:<8}")
            val_p, t_p, mem_p = max_processes(list(data), w)
            ok_p = (val_p == true_max)
            print(f"{w:<8} {'Processes':<12} {t_p:<12.6f} {mem_p:<10.3f} {ok_p!s:<8}")

# ------------------- Entrypoint -------------------
if __name__ == "__main__":
    # Ensure spawn is used on macOS / safety with child process creation
    try:
        mp.set_start_method("spawn")
    except RuntimeError:
        # start method may already be set; ignore
        pass

    run_experiments()