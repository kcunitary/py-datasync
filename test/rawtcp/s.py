import socket
import lz4.frame
chunk_size = 1024 * 1024 * 100
server_port = 2099

def readnbyte (sock, n):
    buff = bytearray (n)
    pos = 0
    while pos < n:
        cr = sock.recv_into(memoryview (buff) [pos:])
        if cr == 0:
            raise EOFError
        pos += cr
    return buff

def receive_file(filename, server_port):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', server_port))
    server_socket.listen(1)

    client_socket, addr = server_socket.accept()

    with open(filename, 'wb') as f:
        while True:
            """header = client_socket.recv(4)  # 4 bytes for sequence number and 4 bytes for filename length
            if not header:
                break
            size = int.from_bytes(header[:4], 'big')
            print(size)
            compressed_chunk = readnbyte(client_socket,size)
            print(len(compressed_chunk))
            chunk = lz4.frame.decompress(compressed_chunk)
            """
            data = client_socket.recv(chunk_size)
            if not data:
                break
            print(len(data))
            f.write(data)

    client_socket.close()
    server_socket.close()


receive_file("/opt/vmTransfer/dataTransfer/python/py-datasync/test/data/dst/1.data",server_port)
# 使用方法：receive_file('receivedfile.txt', 12345)
