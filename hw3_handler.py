import grpc
import csci4220_hw3_pb2
import csci4220_hw3_pb2_grpc
import grpc
import socket
from hw3_routing import RoutingTable

class KadImplServicer(csci4220_hw3_pb2_grpc.KadImplServicer):
    def __init__(self, FindNodeRPC, FindValueRPC, StoreRPC, QuitRPC):
        self.FindNodeRPC = FindNodeRPC
        self.FindValueRPC = FindValueRPC
        self.StoreRPC = StoreRPC
        self.QuitRPC = QuitRPC

    def FindNode(self, request : csci4220_hw3_pb2.IDKey, context):
        return self.FindNodeRPC(request.node, request.idkey)

    def FindValue(self, request, context):
        return self.FindValueRPC(request, context)

    def Store(self, request, context):
        return self.StoreRPC(request, context)

    def Quit(self, request, context):
        return self.QuitRPC(request, context)


class KadEventHandler(object):
    def __init__(self, me: csci4220_hw3_pb2.Node, K: int):
        self.me = me 
        self.K = K
        self.routing_table = RoutingTable(N=4, K=K, me=me)
        self.store = dict()
        self.BOOTSTRAP = self.bootstrap
        self.FIND_NODE = self.find_node
        self.FIND_VALUE = self.find_value
        self.STORE = self.store
        self.QUIT = self.quit

    def FindNodeRPC(self, node : csci4220_hw3_pb2.Node, idkey : int):
        print(f"Serving FindNode({idkey}) request for {node.id}")
        try:
            self.routing_table.make_mru(node.id)
        except KeyError:
            self.routing_table.put(node)
        kclosest = self.routing_table.k_closest(idkey)
        return csci4220_hw3_pb2.NodeList(responding_node=self.me, nodes=kclosest)

    def FindValueRPC(self, node : csci4220_hw3_pb2.Node, idkey : int):
        print(f"Serving FindValue({idkey}) request for {node.id}")
        try:
            self.routing_table.make_mru(node.id)
        except KeyError:
            self.routing_table.put(node)

        if idkey in self.store:
            kv = csci4220_hw3_pb2.KeyValue(node=self.me, key=idkey, value=self.store[idkey])
            return csci4220_hw3_pb2.KV_Node_Wrapper(responding_node=self.me, mode_kv=True, kv=kv, nodes=[])
        
        kclosest = self.routing_table.k_closest(idkey)
        return csci4220_hw3_pb2.NodeList(responding_node=self.me, nodes=kclosest)
        

    def StoreRPC(self, request, context):
        pass
    def QuitRPC(self, request, context):
        pass

    def bootstrap(self, remote_hostname: str, remote_port: int):
        remote_addr = socket.gethostbyname(remote_hostname)
        remote_uri = f"{remote_addr}:{remote_port}"
        with grpc.insecure_channel(remote_uri) as chan:
            stub = csci4220_hw3_pb2_grpc.KadImplStub(chan)
            idkey = csci4220_hw3_pb2.IDKey(node=self.me, idkey=self.me.id)
            nodelist = stub.FindNode(idkey)
            self.routing_table.put(nodelist.responding_node)
            for node in nodelist.nodes:
                self.routing_table.put(node)

            print(f"After BOOTSTRAP({nodelist.responding_node.id}), k-buckets are:")
            print(self.routing_table.buckets_to_str())

    def find_node(self, node_id: int):
        print(f'Before FIND_NODE command, k-buckets are:\n{self.routing_table.buckets_to_str()}')
        found = (self.me.id == node_id)
        asked = set()
        while not found:
            Sp = [node for node in self.routing_table.k_closest(id=node_id) if node.id not in asked]
            if len(Sp) == 0:
                break
            for node in Sp:
                remote_uri = f"{node.address}:{node.port}"
                with grpc.insecure_channel(remote_uri) as chan:
                    stub = csci4220_hw3_pb2_grpc.KadImplStub(chan)
                    idkey = csci4220_hw3_pb2.IDKey(node=self.me, idkey=node_id)
                    nodelist = stub.FindNode(idkey)
                    try:
                        self.routing_table.make_mru(nodelist.responding_node.id)
                    except KeyError:
                        self.routing_table.put(nodelist.responding_node)

                    asked.add(nodelist.responding_node.id)
                    if nodelist.responding_node.id == node_id:
                        found = True
                    for _node in nodelist.nodes:
                        self.routing_table.put(_node)
                        if _node.id == node_id:
                            found = True
        
        if found:
            print(f'Found destination id {node_id}')
        else:
            print(f'Could not find destination id {node_id}')
        
        print(f'After FIND_NODE command, k-buckets are:\n{self.routing_table.buckets_to_str()}')
    
    def find_value(self, key: int):
        print(f'Before FIND_VALUE command, k-buckets are:\n{self.routing_table.buckets_to_str()}')
        value = self.store[key] if key in self.store else None
        no_search = True if value is not None else False # deep copy
        asked = set()
        while value is None:
            Sp = [node for node in self.routing_table.k_closest(id=key) if node.id not in asked]
            if len(Sp) == 0:
                break
            for node in Sp:
                remote_uri = f"{node.address}:{node.port}"
                with grpc.insecure_channel(remote_uri) as chan:
                    stub = csci4220_hw3_pb2_grpc.KadImplStub(chan)
                    idkey = csci4220_hw3_pb2.IDKey(node=self.me, idkey=key)
                    kv_nodelist = stub.FindValue(idkey)
                    try:
                        self.routing_table.make_mru(kv_nodelist.responding_node.id)
                    except KeyError:
                        self.routing_table.put(kv_nodelist.responding_node)
                    asked.add(kv_nodelist.responding_node.id)

                    if kv_nodelist.mode_kv:
                        value = kv_nodelist.kv.value
                    else:
                        for _node in kv_nodelist.nodes:
                            self.routing_table.put(_node)
        
        if no_search:
            print(f'Found data \"{value}\" for key {key}')
        elif value is not None:
            print(f'Found value \"{value}\" for key {key}')
        else:
            print(f'Could not find key {key}')
        print(f'After FIND_VALUE command, k-buckets are:\n{self.routing_table.buckets_to_str()}')


    def store(self, key: int, value: str):
        closest_id = self.routing_table.k_closest(id=key)
        if 
    def quit(self):
        print(f"QUIT")

    def __getitem__(self, item: str):
        return getattr(self, item)