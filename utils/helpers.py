import tracemalloc

def split_into_chunks(data, num_chunks):
    n = len(data)
    base = n // num_chunks
    rem = n % num_chunks
    chunks = []
    start = 0
    for i in range(num_chunks):
        end = start + base + (1 if i < rem else 0)
        chunks.append(data[start:end])
        start = end
    return chunks

def measure_memory_mb():
    current, _ = tracemalloc.get_traced_memory()
    return current / (1024 * 1024)

def is_sorted(arr):
    return all(arr[i] <= arr[i + 1] for i in range(len(arr) - 1))