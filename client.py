""" import os
import zlib
import socket
import time
import logging
from multiprocessing import Pool

logging.basicConfig(filename='client.log', level=logging.INFO)

def compress_and_send(args):
    chunk, start_byte, end_byte, filename, client_socket = args
    compressed_chunk = zlib.compress(chunk)
    client_socket.sendall(start_byte.to_bytes(8, 'big') + end_byte.to_bytes(8, 'big') + len(filename).to_bytes(4, 'big') + filename.encode() + compressed_chunk)
    logging.info(f'Sent bytes {start_byte} to {end_byte} of file {filename}')

def send_files(filenames, server_ip, server_port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_ip, server_port))

    pool = Pool(processes=4)  # 创建一个进程池

    for filename in filenames:
        start_byte = 0
        start_time = time.time()
        with open(filename, 'rb') as f:
            while True:
                chunk = f.read(1024)
                if not chunk:
                    break
                end_byte = start_byte + len(chunk) - 1
                pool.apply_async(compress_and_send, ((chunk, start_byte, end_byte, filename, client_socket),))
                start_byte = end_byte + 1
        end_time = time.time()
        logging.info(f'Finished sending file {filename} in {end_time - start_time} seconds')

    pool.close()
    pool.join()

    client_socket.close() """

# 使用方法：send_files(['largefile1.txt', 'largefile2.txt'], '127.0.0.1', 12345)
import grpc
import data_check_pb2
import data_check_pb2_grpc

def run():
    with open('file_to_upload', 'rb') as f:
        data = f.read()

    channel = grpc.insecure_channel('localhost:50051')
    stub = data_check_pb2_grpc.FileServiceStub(channel)
    response = stub.UploadFile(data_check_pb2.FileCheckRequest(name='file_to_upload', data=data))

    if response.success:
        print('File uploaded successfully.')
    else:
        print('File upload failed.')

if __name__ == '__main__':
    run()
