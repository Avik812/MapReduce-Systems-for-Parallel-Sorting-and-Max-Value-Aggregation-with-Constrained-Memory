import random
from mapreduce_sort.map_sort_threaded import map_sort_threaded
from mapreduce_sort.map_sort_process import map_sort_process
from mapreduce_sort.reducer_merge import reducer_merge
from max_value_aggregation.max_threaded import max_threaded
from max_value_aggregation.max_process import max_process
from utils.helpers import is_sorted

def run_experiments():
    sizes = [32, 131072]
    workers = [1, 2, 4, 8]
    print("\n MapReduce Parallel Sorting \n")

    for size in sizes:
        data = [random.randint(0, 1000000) for _ in range(size)]
        print(f"Input size: {size}")
        print(f"{'Workers':<8}{'Mode':<12}{'Time(s)':<12}{'Heap(MB)':<10}{'Correct':<8}")
        print("-" * 60)

        for w in workers:
            # Threads
            chunks, t, mem = map_sort_threaded(list(data), w)
            sorted_arr = reducer_merge(chunks)
            print(f"{w:<8}{'Threads':<12}{t:<12.6f}{mem:<10.3f}{is_sorted(sorted_arr)}")

            # Processes
            chunks, t, mem = map_sort_process(list(data), w)
            sorted_arr = reducer_merge(chunks)
            print(f"{w:<8}{'Processes':<12}{t:<12.6f}{mem:<10.3f}{is_sorted(sorted_arr)}")

        print()

    print(" Max-Value Aggregation \n")

    for size in sizes:
        data = [random.randint(0, 1000000) for _ in range(size)]
        correct = max(data)
        print(f"Input size: {size}")
        print(f"{'Workers':<8}{'Mode':<12}{'Time(s)':<12}{'Heap(MB)':<10}{'Correct':<8}")
        print("-" * 60)

        for w in workers:
            val, t, mem = max_threaded(list(data), w)
            print(f"{w:<8}{'Threads':<12}{t:<12.6f}{mem:<10.3f}{val == correct}")

            val, t, mem = max_process(list(data), w)
            print(f"{w:<8}{'Processes':<12}{t:<12.6f}{mem:<10.3f}{val == correct}")

        print()