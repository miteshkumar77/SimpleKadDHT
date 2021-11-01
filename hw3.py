#!/usr/bin/env python3

from concurrent import futures
import sys  # For sys.argv, sys.exit()
import socket  # for gethostbyname()
import grpc
from typing import List
from hw3_handler import KadImplServicer
import csci4220_hw3_pb2
import csci4220_hw3_pb2_grpc

# create a kad grpc server and return it, as well as it's user command interface
def create_kad_server(local_id : int, my_port : str, my_address : str, k : int, file=sys.stdout):
    handler = KadImplServicer(csci4220_hw3_pb2.Node(id=local_id, port=int(my_port), address=my_address), k, file)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    csci4220_hw3_pb2_grpc.add_KadImplServicer_to_server(handler, server)
    port = server.add_insecure_port(f'[::]:{my_port}')
    return (server, handler)

def run():
    if len(sys.argv) != 4:
        print("Error, correct usage is {} [my id] [my port] [k]".format(
            sys.argv[0]))
        sys.exit(-1)

    local_id: int = int(sys.argv[1])
    # add_insecure_port() will want a string
    my_port: str = str(int(sys.argv[2]))
    k: int = int(sys.argv[3])
    my_hostname: str = socket.gethostname()  # Gets my host name
    # Gets my IP address from my hostname
    my_address: str = socket.gethostbyname(my_hostname)
    ''' Use the following code to convert a hostname to an IP and start a channel
	Note that every stub needs a channel attached to it
	When you are done with a channel you should call .close() on the channel.
	Submitty may kill your program if you have too many file descriptors open
	at the same time. '''
    server, handler = create_kad_server(local_id=local_id, my_port=my_port, my_address=my_address, k=k)
    server.start()

    while True:
        # loop and accept commands from the user
        command_list = sys.stdin.readline().strip('\n').strip(' ').split(' ')
        command = command_list[0]
        args = command_list[1:]
        for idx, elem in enumerate(args):
            if elem.isnumeric():
                args[idx] = int(elem)

        handler[command](*args)
        
        # finish execution after QUIT command
        if command == 'QUIT':
            break
    
    server.stop(grace=None)

if __name__ == '__main__':
    run()
