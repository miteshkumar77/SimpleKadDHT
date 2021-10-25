import csci4220_hw3_pb2
import csci4220_hw3_pb2_grpc
import grpc
from collections import OrderedDict
from typing import List, Dict, Set
from math import ceil, log2


def kad_distance(id1: int, id2: int):
    return id1 ^ id2


def get_bucket_idx(ownID: int, insertId: int):
    return int(ceil(log2(kad_distance(ownID, insertId)))) - 1

class LRUCache():
    def __init__(self, MaxSize: int):
        self.N: int = MaxSize
        self.ids: OrderedDict = OrderedDict()

    def make_lru(self, id: int):
        self.ids.move_to_end(id, last=False)

    def put(self, id: int, addr: csci4220_hw3_pb2.Node):
        if id not in self.ids:
            self.ids[id] = addr
            if len(self.ids) > self.N:
                self.ids.popitem(last=True)

    def list_lru_items(self):
        return self.ids.values()

    def list_sorted_items(self, srcID: int):
        distance_key = lambda item : kad_distance(srcID, item.id)
        return sorted([item for item in self.ids.values()], key=distance_key)

class KBuckets():
    def __init__(self, K: int, N: int, ownID: int):
        self.ownID = ownID
        self.buckets: List[LRUCache] = [LRUCache(K) for _ in range(N)]
    
    def k_closest(self, nodeID : int):
        k = len(self.buckets)
        if k == 0:
            return
        for bucket in self.buckets:
            for item in bucket.list_sorted_items(nodeID):
                yield item
                k -= 1
                if k == 0:
                    return

    def make_lru(self, id: int):
        self.buckets[get_bucket_idx(self.ownID, id)].make_lru(id)

    def put(self, id: int, addr : csci4220_hw3_pb2.Node):
        self.buckets[get_bucket_idx(self.ownID, id)].put(id, addr)

    def __repr__(self):
        def item_str(item): return f"{item[0]}:{item[1].port}"
        def bucket_str(bucket): return ' '.join(item_str(item)
                                                for item in bucket.list_lru_items())
        return '\n'.join(f"{idx} [{bucket_str(bucket)}]" for idx, bucket in enumerate(self.buckets))


class KadImplServicer(csci4220_hw3_pb2_grpc.KadImplServicer):
    def __init__(self, FindNodeRPC, FindValueRPC, StoreRPC, QuitRPC):
        self.FindNodeRPC = FindNodeRPC
        self.FindValueRPC = FindValueRPC
        self.StoreRPC = StoreRPC
        self.QuitRPC = QuitRPC

    def FindNode(self, request, context):
        return self.FindNodeRPC(request, context)

    def FindValue(self, request, context):
        return self.FindValueRPC(request, context)

    def Store(self, request, context):
        return self.StoreRPC(request, context)

    def Quit(self, request, context):
        return self.QuitRPC(request, context)


class KadEventHandler(object):
    def __init__(self, own_addr: csci4220_hw3_pb2.Node, k: int):
        self.me = own_addr
        self.KBuckets = KBuckets(k, 4, self.me.id)
        self.BOOTSTRAP = self.bootstrap
        self.FIND_NODE = self.find_node
        self.FIND_VALUE = self.find_value
        self.STORE = self.store
        self.QUIT = self.quit

    def FindNodeRPC(self, requester: csci4220_hw3_pb2.Node, nodeID : int):
        print(f"Serving FindNode({nodeID}) request for {requester.id}")
        return csci4220_hw3_pb2.NodeList(responding_node=self.me, nodes=self.KBuckets.k_closest(nodeID))
    def FindValueRPC(self, request, context):
        pass
    def StoreRPC(self, request, context):
        pass
    def QuitRPC(self, request, context):
        pass

    def bootstrap(self, remote_hostname: str, remote_port: int):
        pass

    def find_node(self, node_id: int):
        print(f"Before FIND_NODE command, k-buckets are:\n{str(self.KBuckets)}")
        found : bool = (node_id == self.me.id)
        print(f"ownid: {self.me.id}")
        if not found:
            asked : Set[int] = set()
            while True:
                found = False
                Sp : Dict[int, csci4220_hw3_pb2.Node] = {node.id:node for node in self.KBuckets.k_closest(self.me.id) if key not in asked}
                if len(Sp) == 0:
                    break
                for key, val in Sp.items():
                    if key == node_id:
                        found = True
                    asked.add(key)
                    with grpc.insecure_channel(val.address + ":" + str(val.port)) as channel:
                        stub = csci4220_hw3_pb2_grpc.KadImplStub(channel)
                        idkey = csci4220_hw3_pb2.IDKey(node=self.me, idkey=node_id)
                        nodelist = stub.FindNode(idkey)
                        self.KBuckets.make_lru(nodelist.responding_node.id)
                        for node in nodelist.nodes:
                            self.KBuckets.put(node.id, node)
                            if node.id == node_id:
                                found = True
                if found:
                    break
        if found:
            print(f"Found destination id {node_id}")
        else:
            print(f"Could not find destination id {node_id}")
        
        print(f"After FIND_NODE command, k-buckets are:\n{str(self.KBuckets)}")
            

    
    def find_value(self, key: str):
        print(f"FIND_VALUE {key}")

    def store(self, key: str, value: str):
        print(f"STORE {key} {value}")

    def quit(self):
        print(f"QUIT")

    def __getitem__(self, item: str):
        return getattr(self, item)
