from math import floor, log2

def distance(own_id: int, id: int):
    return own_id ^ id

def id_to_str(id : int, N : int):
    return ("{0:b}".format(id)).rjust(N, '0')

def str_to_id(id_str : str):
    return int(id_str, base=2)

# 2^idx <= dist < 2^(idx+1)
# idx <= log_2(dist)
# idx > log_2(dist) - 1
# idx = floor(log_2(dist))
def get_bucket_idx(own_id: int, id : int):
    if own_id == id:
        return -1
    return int(floor(log2(distance(own_id, id))))