import zlib
import logging
import grpc,threading,pathlib
from concurrent import futures
import data_check_pb2
import data_check_pb2_grpc
import storge_cdc,local_storage_scan,hashlib


#logging.basicConfig(filename='server.log', level=logging.INFO)
log_formater = '%(threadName)s - %(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
logging.basicConfig(filename='server.log',level=logging.DEBUG, format=log_formater)

#conf
server_addr = '[::]:50051'
max_rpc_workers=30
MAX_MESSAGE_LENGTH = 150 * 1024 * 1024
""" rpc_options = [
 #   ('grpc.max_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)
] """
rpc_options = [
    ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)
]
local_resource_dir = "/opt/vmTransfer/dataTransfer/python/testdata/res"
recive_dst_path = "/opt/vmTransfer/dataTransfer/python/testdata/dst"



def gen_path(raw_path):
    raw_path = pathlib.Path(raw_path)
    new_dir = pathlib.Path(recive_dst_path) / raw_path.parent.name
    dst_path = new_dir / raw_path.name
    pathlib.Path(new_dir).mkdir(parents=True,exist_ok=True)
    return dst_path
class FileServiceServicer(data_check_pb2_grpc.FileServiceServicer):
    def CheckFile(self, request, context):
        logging.debug(f"checkfile request:{request}")
        result = local_file_segment.get(request.hash,None)
        newpath =gen_path(request.path)

        if result:
            logging.info(f"resource found in local:{result}")
            logging.debug(f"dest path:{newpath}")
            try:
                with open(result.path,"rb") as f:
                    f.seek(result.start_pos)
                    data = f.read(result.length)
                if not data:
                    raise Exception("data empyt")
                read_data_hash = hashlib.md5(data).hexdigest()
                if read_data_hash != request.hash:
                    raise Exception(f"data changed:{read_data_hash}")
                with file_lock:
                    with open(newpath, 'wb') as f:
                        f.seek(request.start_pos)
                        f.write(data)
                return data_check_pb2.FileCheckResponse(exists=True)
            except Exception as err:
                logging.error(f"local resoure err:{err}",exc_info=True)
                return data_check_pb2.FileCheckResponse(exists=False)
        else:
            return data_check_pb2.FileCheckResponse(exists=False)

        

    def UploadFile(self, request, context):
        npath = gen_path(request.path)
        decompressed_data = zlib.decompress(request.data)
        logging.debug(f"upload request:{request.path,request.length}")
        try:
            with file_lock:
                logging.debug(f"write path:{npath}")
                with open(npath, 'wb') as f:
                    f.seek(request.start_pos)
                    f.write(decompressed_data)
            with info_lock:
                if request.hash not in local_file_segment:
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
        except Exception as err:
            logging.error(f"receive data error:{err}",exc_info=True)

def serve(server_addr):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_rpc_workers),options = rpc_options)
    data_check_pb2_grpc.add_FileServiceServicer_to_server(FileServiceServicer(), server)
    server.add_insecure_port(server_addr)
    server.start()
    server.wait_for_termination()



if __name__ == '__main__':
    #TO DO:each file lock
    file_lock = threading.Lock()
    #info lock
    info_lock = threading.Lock()
    print("program is starting....")
    local_file_segment = storge_cdc.scan_directory(local_resource_dir)
    local_file_segment = {x.hash:x for x in local_file_segment}
    logging.info(f"init local {len(local_file_segment)} file segments")
    print(f"init local {len(local_file_segment)} file segments")
    serve(server_addr)