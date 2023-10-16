import abc,logging
#from typing import Any, Dict, AsyncGenerator
from dataclasses import dataclass
import pickle
import grpc
import socket
from  ..common.grpc import data_check_pb2_grpc,data_check_pb2
from ..common.dataclass import transform_data_class

MAX_MESSAGE_LENGTH = 150 * 1024 * 1024
rpc_options = [
    ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)
]




class client(abc.ABC):
    def __init__(self, server_addr,server_port) -> None:
        self.addr = server_addr
        self.port = server_port
#        self.con = self.connect(server_addr,server_port)
    def connect(self,addr,port):
        pass
    def check(self,request):
        pass
    def upload(self,request):
        pass


class grpc_tcp_client(client):
#    def connect(self,addr,port):
#        channel = grpc.insecure_channel(f"{self.addr}:{self.port}",options=rpc_options)
#        stub = data_check_pb2_grpc.FileServiceStub(channel)
#        self.con = stub
    def __init__(self, server_addr,server_port,data_port) -> None:
        self.addr = server_addr
        self.port = server_port
        self.data_port = data_port
    def check(self,check_request):
        channel = grpc.insecure_channel(f"{self.addr}:{self.port}",options=rpc_options)
        stub = data_check_pb2_grpc.FileServiceStub(channel)
        self.con = stub
        request_dict = check_request.__dict__
        request = data_check_pb2.FileCheckRequest(**request_dict)
        response = self.con.CheckFile(request)
        channel.close()
        return response
    def upload(self,request):
        length = len(request.data)
        header  = request.id.to_bytes(4, 'big') + length.to_bytes(4, 'big')
        data = request.data
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.addr, self.data_port))
            resp = s.sendall(header)
            resp  = s.sendall(data)
            resp_data = b''
            while True:
                p = s.recv(1024)
                if not p:
                    break
                resp_data += p
        resonse = pickle.loads(resp_data)
        return resonse