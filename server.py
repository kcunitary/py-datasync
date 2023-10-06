import socketserver
import zlib
import time
import logging

logging.basicConfig(filename='server.log', level=logging.INFO)


import grpc,threading,pathlib
from concurrent import futures
import data_check_pb2
import data_check_pb2_grpc
import storge_cdc,local_storage_scan

dst_path = "/opt/synctest"
def gen_path(p):
    path = pathlib.Path(p)
    p = dst_path / path.parent /path.name
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
    logging.debug(f"local file segment:{local_file_segment.items()}")
    logging.info(f"init local {len(local_file_segment)} file segments")
    serve()
