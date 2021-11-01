import grpc
import csci4220_hw3_pb2
import csci4220_hw3_pb2_grpc
import grpc
import socket
from hw3_routing import RoutingTable
from hw3_utils import distance, get_bucket_idx
import sys

class KadImplServicer(csci4220_hw3_pb2_grpc.KadImplServicer, object):
    def __init__(self, me: csci4220_hw3_pb2.Node, K: int, file):
        self.me = me 
        self.K = K
        self.routing_table = RoutingTable(N=4, K=K, me=me)
        self.kv_store = dict()
        self.BOOTSTRAP = self.bootstrap
        self.FIND_NODE = self.find_node
        self.FIND_VALUE = self.find_value
        self.STORE = self.store
        self.QUIT = self.quit
        self.file = file
           
    # RPC handler for FindNode message
    def FindNodeRPC(self, node : csci4220_hw3_pb2.Node, idkey : int):
        print(f"Serving FindNode({idkey}) request for {node.id}", file=self.file)
        kclosest = self.routing_table.k_closest(idkey)
        self.routing_table.put(node)
        self.routing_table.make_mru(node.id)
        return csci4220_hw3_pb2.NodeList(responding_node=self.me, nodes=kclosest)

    # RPC handler for FindValue message
    def FindValueRPC(self, node : csci4220_hw3_pb2.Node, idkey : int):
        print(f"Serving FindKey({idkey}) request for {node.id}", file=self.file)

        if idkey in self.kv_store:
            kv = csci4220_hw3_pb2.KeyValue(node=self.me, key=idkey, value=self.kv_store[idkey])
            res = csci4220_hw3_pb2.KV_Node_Wrapper(responding_node=self.me, mode_kv=True, kv=kv)
        else:
            kclosest = self.routing_table.k_closest(idkey)
            res = csci4220_hw3_pb2.KV_Node_Wrapper(responding_node=self.me, mode_kv=False, nodes=kclosest)

        self.routing_table.put(node)
        self.routing_table.make_mru(node.id)

        return res
        
    # RPC handler for Store message
    def StoreRPC(self, node : csci4220_hw3_pb2.Node, key: int, value: str):
        print(f'Storing key {key} value "{value}"', file=self.file)
        
        self.kv_store[key] = value
        self.routing_table.put(node)
        self.routing_table.make_mru(node.id)
        return csci4220_hw3_pb2.IDKey(node=self.me, idkey=key) # return not used

    # RPC handler for Quit message
    def QuitRPC(self, id : int):
        try:
            self.routing_table.remove(id)
            print(f'Evicting quitting node {id} from bucket {get_bucket_idx(self.me.id, id)}', file=self.file)
        except KeyError:
            print(f'No record of quitting node {id} in k-buckets.', file=self.file)
        return csci4220_hw3_pb2.IDKey(node=self.me, idkey=id)

    # handler for BOOTSTRAP command
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

            print(f"After BOOTSTRAP({nodelist.responding_node.id}), k-buckets are:", file=self.file)
            print(self.routing_table.buckets_to_str(), file=self.file)

    # handler for FIND_NODE command
    def find_node(self, node_id: int):
        print(f'Before FIND_NODE command, k-buckets are:\n{self.routing_table.buckets_to_str()}', file=self.file)
        found = (self.me.id == node_id)

        # set of node ids that we have already made FindNode RPC calls to
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
                    self.routing_table.put(nodelist.responding_node)
                    self.routing_table.make_mru(nodelist.responding_node.id)
                    
                    asked.add(nodelist.responding_node.id)
                    found = (found or nodelist.responding_node.id == node_id)
                    for _node in nodelist.nodes:
                        self.routing_table.put(_node)
                        # finish updating k-buckets even if we have
                        # found the node with id node_id
                        found = (found or (_node.id == node_id))
        
        if found:
            print(f'Found destination id {node_id}', file=self.file)
        else:
            print(f'Could not find destination id {node_id}', file=self.file)
        
        print(f'After FIND_NODE command, k-buckets are:\n{self.routing_table.buckets_to_str()}', file=self.file)

    # handler for FIND_VALUE command
    def find_value(self, key: int):
        print(f'Before FIND_VALUE command, k-buckets are:\n{self.routing_table.buckets_to_str()}', file=self.file)
        value = self.kv_store[key] if key in self.kv_store else None
        no_search = True if value is not None else False # deep copy

        # set of node ids that we have already made FindValue RPC calls to
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
                    self.routing_table.put(kv_nodelist.responding_node)
                    self.routing_table.make_mru(kv_nodelist.responding_node.id)
                    asked.add(kv_nodelist.responding_node.id)

                    # only update either value or routing_table
                    if kv_nodelist.mode_kv:
                        value = kv_nodelist.kv.value
                        break
                    else:
                        for _node in kv_nodelist.nodes:
                            self.routing_table.put(_node)
        
        if no_search:
            print(f'Found data \"{value}\" for key {key}', file=self.file)
        elif value is not None:
            print(f'Found value \"{value}\" for key {key}', file=self.file)
        else:
            print(f'Could not find key {key}', file=self.file)
        print(f'After FIND_VALUE command, k-buckets are:\n{self.routing_table.buckets_to_str()}', file=self.file)

    # handler for STORE command
    def store(self, key: int, value: str):

        # get the closest id to key using xor as distance
        closest_node_arr = self.routing_table.n_closest(id=key, n=1)
        closest_node = self.me if len(closest_node_arr) == 0 else closest_node_arr[0]

        # store locally if own id is the closest
        if distance(closest_node.id , key) >= distance(self.me.id, key):
            closest_node = self.me
            self.kv_store[key] = value
        else:
            remote_uri = f"{closest_node.address}:{closest_node.port}"
            with grpc.insecure_channel(remote_uri) as chan:
                stub = csci4220_hw3_pb2_grpc.KadImplStub(chan)
                kv = csci4220_hw3_pb2.KeyValue(node=self.me, key=key, value=value)
                stub.Store(kv)
        print(f'Storing key {key} at node {closest_node.id}', file=self.file)
    
    # handler for QUIT command
    def quit(self):
        for node in self.routing_table.all_nodes():
            remote_uri = f"{node.address}:{node.port}"
            print(f'Letting {node.id} know I\'m quitting.', file=self.file)

            # if the node didn't contain us in their k-buckets at the time
            # it shut down, then we may still have it in our k-buckets.
            #
            # don't allow failure here if the Quit RPC message fails to send.
            try:
                with grpc.insecure_channel(remote_uri) as chan:
                    stub = csci4220_hw3_pb2_grpc.KadImplStub(chan)
                    idkey = csci4220_hw3_pb2.IDKey(node=self.me, idkey=self.me.id)
                    stub.Quit(idkey)
            except Exception:
                continue
        print(f'Shut down node {self.me.id}', file=self.file)

    # helper method to make KadImplServicer object subscriptable
    def __getitem__(self, item: str):
        return getattr(self, item)


    ### Wrappers ###

    def FindNode(self, request : csci4220_hw3_pb2.IDKey, context):
        return self.FindNodeRPC(request.node, request.idkey)

    def FindValue(self, request : csci4220_hw3_pb2.IDKey, context):
        return self.FindValueRPC(request.node, request.idkey)

    def Store(self, request : csci4220_hw3_pb2.KeyValue, context):
        return self.StoreRPC(request.node, request.key, request.value)

    def Quit(self, request : csci4220_hw3_pb2.IDKey, context):
        return self.QuitRPC(request.idkey)