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
import storge_cdc,local_storage_scan

def gen_path(p):
    return p
class FileServiceServicer(data_check_pb2_grpc.FileServiceServicer):
    def CheckFile(self, request, context):

        result = local_file_segment.get(result.hash,None)
        newpath =gen_path(request.path)
        if result:
            with open(result.path,"rb") as f:
                f.seek(result.start_pos)
                data = f.read(result.length)
            with file_lock:
                with open(newpath, 'wb') as f:
                    f.seek(request.start_pos)
                    f.write(data)
            return data_check_pb2.FileCheckResponse(exists=True)
        else:
            return data_check_pb2.FileCheckResponse(exists=False)

        

    def UploadFile(self, request, context):
        npath = gen_path(request.path)
        decompressed_data = zlib.decompress(request.data)
        with file_lock:
            with open(npath, 'wb') as f:
                f.seek(request.start_pos)
                f.write(decompressed_data)
        with info_lock:
            local_file_segment[request.hash] = local_storage_scan.FileFragment(
                path=npath,
                start_pos=request.start_pos,
                length = request.length,
                total_size = request.total_size,
                mtime = request.mtime,
                hash_type = request.hash_type,
                hash  = request.hash
            )
        return data_check_pb2.FileUploadResponse(success=True)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=30))
    data_check_pb2_grpc.add_FileServiceServicer_to_server(FileServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()
local_resource_dir = "/opt"

if __name__ == '__main__':
    #TO DO:each file lock
    file_lock = threading.Lock()
    #info lock
    info_lock = threading.Lock()
    local_file_segment = storge_cdc.scan_directory(local_resource_dir)
    local_file_segment = {x.hash:x for x in local_file_segment}
    print(len(local_file_segment))
    serve()
