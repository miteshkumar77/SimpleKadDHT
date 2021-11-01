from collections import OrderedDict
import sys
from typing import List
import csci4220_hw3_pb2
import csci4220_hw3_pb2_grpc
from hw3_utils import distance, get_bucket_idx, str_to_id, id_to_str

# LRU Cache using linked hash set
class LRUCache():
    def __init__(self, MaxSize : int, call_on_evict, call_on_add):
        self.MaxSize: int = MaxSize
        self.lru_list: OrderedDict = OrderedDict()
        self.call_on_evict = call_on_evict
        self.call_on_add = call_on_add
    
    # insert a key/value as the most recently used element
    # evict the least recently used element if needed, and
    # call the on_evict handler
    # if the key already exists, nothing happens
    def put(self, key: int, value):
        if key not in self.lru_list:
            self.lru_list[key] = value
            self.call_on_add(key)
            if len(self.lru_list) > self.MaxSize:
                self.call_on_evict(self.lru_list.popitem(last=False)[0])
    
    # remove an element with a particular key from the cache
    # if an element with key does not exist, raise KeyError
    def remove(self, key : int):
        if key in self.lru_list:
            del self.lru_list[key]
            self.call_on_evict(key)
        else:
            raise KeyError
    
    # mark element with key as most recently used
    # if it doesn't exist, raise KeyError
    def make_mru(self, key: int):
        self.lru_list.move_to_end(key, last=True)
    
    # list elements in order of least recently used
    # to most recently used
    def list_lru_items(self):
        return self.lru_list.values()
    
    # return an element with a particular key from the cache
    # if such a key does not exist, raise KeyError
    def get(self, key : int):
        return self.lru_list[key]

# BinaryNode used within BinaryTrie for k-closest queries
class BinaryNode():
    def __init__(self):
        self.size = 0
        self.children = [None, None]
        self.isEnd = False

# BinaryTrie used for k-closest queries
class BinaryTrie():
    def __init__(self):
        self.root = BinaryNode()
    
    # add an id to the trie
    def add(self, binary_string : str):
        curr = self.root
        for c in (int(ci) for ci in binary_string):
            if curr.children[c] is None:
                curr.children[c] = BinaryNode()
            curr = curr.children[c]

            curr.size += 1
        curr.isEnd = True
    
    # remove an id from the trie
    def remove(self, binary_string : str):
        prev = None
        curr = self.root
        for c in (int(ci) for ci in binary_string):
            prev = curr
            curr = curr.children[c]
            curr.size -= 1
            if curr.size == 0:
                prev.children[c] = None
                break
    
    # get the k closest elements to a particular id within the trie
    # using xor as distance.
    def k_closest(self, binary_string : str, k : int):
        return self.k_closest_helper(search_binary_string=binary_string, curr_binary_string="", k=k, curr_node=self.root)

    def k_closest_helper(self, search_binary_string: str, curr_binary_string : str, k : int, curr_node : BinaryNode):
        if search_binary_string == "":
            return [curr_binary_string]
        
        c = int(search_binary_string[0])
        cp = c ^ 1
        cp_skip = 0 if curr_node.children[c] is None else curr_node.children[c].size

        ret : List[str] = []
        if curr_node.children[c] is not None:
            ret += self.k_closest_helper(search_binary_string[1:], curr_binary_string + str(c), k, curr_node.children[c])
        if curr_node.children[cp] is not None and cp_skip < k:
            ret += self.k_closest_helper(search_binary_string[1:], curr_binary_string + str(cp), k - cp_skip, curr_node.children[cp])

        return ret

# routing table used to manage k-buckets and provide k-closest queries
# on those k-buckets
class RoutingTable():
    def __init__(self, N: int, K: int, me : csci4220_hw3_pb2.Node):
        self.me = me
        self.N = N
        self.K = K
        self.BinTrie = BinaryTrie()
        deleter = lambda key : self.BinTrie.remove(id_to_str(id=key, N=N))
        inserter = lambda key : self.BinTrie.add(id_to_str(id=key, N=N))
        self.LRUList = [LRUCache(K, deleter, inserter) for _ in range(N)]
    
    # find the n closest elements to an id.
    # 
    # wrapper for k_closest in BinaryTrie
    def n_closest(self, id : int, n) -> List[csci4220_hw3_pb2.Node]:
        n_closest_strids = self.BinTrie.k_closest(id_to_str(id=id, N=self.N), k=n)
        n_closest_ids = [str_to_id(_id) for _id in n_closest_strids]
        return [self.id_lookup(_id) for _id in n_closest_ids]

    def k_closest(self, id : int) -> List[csci4220_hw3_pb2.Node]:
        return self.n_closest(id, self.K)

    # return the LRUCache object that contains an id
    def bucket_of_id(self, id : int):
        return self.LRUList[get_bucket_idx(self.me.id, id)]
    
    def id_lookup(self, id : int):
        if id == self.me.id:
            return self.me
        return self.bucket_of_id(id).get(id)
    
    def remove(self, id : int):
        self.bucket_of_id(id).remove(id)
    
    def put(self, node : csci4220_hw3_pb2.Node):
        if node.id == self.me.id:
            return
        self.bucket_of_id(node.id).put(node.id, node)

    def make_mru(self, id : int):
        if id == self.me.id:
            return
        self.bucket_of_id(id).make_mru(id)
    
    def all_nodes(self):
        for lru_list in self.LRUList:
            for node in lru_list.lru_list.values():
                yield node

    def buckets_to_str(self):
        def make_entry(node : csci4220_hw3_pb2.Node): return f"{node.id}:{node.port}"
        def make_row(lru_cache : LRUCache): return ' '.join(map(make_entry, lru_cache.list_lru_items()))
        def fmt_row(row): return '' if row == '' else ' ' + row
        return '\n'.join(f"{idx}:{fmt_row(make_row(lru_cache))}" for idx, lru_cache in enumerate(self.LRUList))

if __name__ == "__main__":
    B = BinaryTrie()
    B.add("000")
    B.add("001")
    B.add("010")
    B.add("011")
    B.add("100")
    B.add("101")
    B.add("110")
    B.add("111")
    print(B.k_closest("000", 3))
    B.remove("000")
    B.remove("001")
    print(B.k_closest("000", 3))
    print(f"DIST: {distance(0, 1)}, BUCKET: {get_bucket_idx(0, 1)}")
    me=csci4220_hw3_pb2.Node(id=0, port=9000, address="localhost")
    p1=csci4220_hw3_pb2.Node(id=1, port=9001, address="localhost")
    p2=csci4220_hw3_pb2.Node(id=2, port=9002, address="localhost")
    p3=csci4220_hw3_pb2.Node(id=7, port=9003, address="localhost")
    p4=csci4220_hw3_pb2.Node(id=8, port=9004, address="localhost")
    R = RoutingTable(N=4, K=2, me=me)
    print(R.buckets_to_str())
    R.put(p1)
    R.put(p2)
    R.put(p3)
    R.put(p4)
    print(R.buckets_to_str())
    print(R.k_closest(7))
    