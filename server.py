import socketserver
import zlib
import time
import logging

logging.basicConfig(filename='server.log', level=logging.INFO)

""" class MyTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        chunks = {}
        start_time = time.time()
        while True:
            header = self.request.recv(20)  # 8 bytes for start byte, 8 bytes for end byte and 4 bytes for filename length
            if not header:
                break
            start_byte = int.from_bytes(header[:8], 'big')
            end_byte = int.from_bytes(header[8:16], 'big')
            filename_length = int.from_bytes(header[16:], 'big')
            filename = self.request.recv(filename_length).decode()
            compressed_chunk = self.request.recv(1024)
            chunk = zlib.decompress(compressed_chunk)
            if filename not in chunks:
                chunks[filename] = {}
            chunks[filename][(start_byte, end_byte)] = chunk
            logging.info(f'Received bytes {start_byte} to {end_byte} of file {filename}')

        for filename in chunks.keys():
            with open(filename, 'wb') as f:
                for (start_byte, end_byte) in sorted(chunks[filename].keys()):
                    f.write(chunks[filename][(start_byte, end_byte)])
        end_time = time.time()
        logging.info(f'Finished receiving file {filename} in {end_time - start_time} seconds')

def run_server(host, port):
    with socketserver.ThreadingTCPServer((host, port), MyTCPHandler) as server:
        server.serve_forever()

# 使用方法：run_server('0.0.0.0', 12345)

def init_local_data():
    file_list = scan_dir()
    local_file_info = process_files(file_list)
#TO DO: dump load update
    return local_file_info

def handle_send_hash(data):
    return data

def handle_income_file(path,offset,data):
    data = decompress(data)
    writer.write(path,data)
    checksum_all.update(data) """
# 使用方法：scan_directory('你的目录路径')


import grpc,threading
from concurrent import futures
import data_check_pb2
import data_check_pb2_grpc
import storge_cdc


class FileServiceServicer(data_check_pb2_grpc.FileServiceServicer):
    def CheckFile(self, request, context):
        # 检查MD5值是否存在
        # 如果不存在，返回exists=False
        # ...
#add index or dict
        #result = [d for d in local_file_segment if d.get('hash') == result.hash]
        result = local_file_segment.get(result.hash,None)
        if result:
            return data_check_pb2.FileCheckResponse(exists=False)
        else:
            with open(result.path,"rb") as f:
                f.seek(result.start_pos)
                data = f.read(result.length)
            with open(request.path, 'wb') as f:
                f.seek(request.start_pos)
                f.write(data)
            return data_check_pb2.FileCheckResponse(exists=True)
        

    def UploadFile(self, request, context):
        path = request.path
        decompressed_data = zlib.decompress(request.data)
        with open(path, 'wb') as f:
            f.seek(request.start_pos)
            f.write(decompressed_data)
        return data_check_pb2.FileUploadResponse(success=True)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    data_check_pb2_grpc.add_FileServiceServicer_to_server(FileServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()
local_resource_dir = "/opt"

if __name__ == '__main__':
    local_file_segment = storge_cdc.scan_directory(local_resource_dir)
    local_file_segment = {x.hash:x for x in local_file_segment}
    lock_list = []
    logging.info(f'Finished receiving file {filename} in {end_time - start_time} seconds')
    serve()
