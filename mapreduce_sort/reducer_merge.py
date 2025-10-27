import heapq

def reducer_merge(sorted_chunks):
    return list(heapq.merge(*sorted_chunks))