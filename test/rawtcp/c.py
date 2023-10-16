import os
import lz4.frame
import socket
chunk_size = 1024 * 1024 * 100
server_port = 2099
path = "/opt/vmTransfer/dataTransfer/python/py-datasync/test/data/test-img/win7 sp1 旗舰版 2020.05 x64.iso"
def send_file(filename, server_ip, server_port):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((server_ip, server_port))

    with open(filename, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
#            compressed_chunk = lz4.frame.compress(chunk,block_size=lz4.frame.BLOCKSIZE_MAX4MB,content_checksum=True)
#            size = len(compressed_chunk)
 #           header  = size.to_bytes(4, 'big')
            client_socket.sendall(chunk)

    client_socket.close()

import asyncio
import socket

async def send_file_to_server(file_path, server_ip, server_port):
    reader, writer = await asyncio.open_connection(server_ip, server_port)

    with open(file_path, 'rb') as file:
        while True:
            data = file.read(chunk_size)
            if not data:
                break
            writer.write(data)
            
    await writer.drain()
    print("Closing the connection")
    writer.close()
    await writer.wait_closed()

# 使用示例
asyncio.run(send_file_to_server(path, '127.0.0.1', server_port))
#send_file(path, '127.0.0.1', server_port)
# 使用方法：send_file('largefile.txt', '127.0.0.1', 12345)
