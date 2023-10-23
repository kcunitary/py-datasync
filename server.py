#import lz4.frame,zstd
import logging,grpc
import threading,pathlib
from concurrent import futures
from src.common.grpc import data_check_pb2_grpc,data_check_pb2
from src.server import storge_cdc
from src.common.dataclass import transform_data_class
import hashlib
import socketserver
import multiprocessing
#from internal.common.dataclass import  transform_data_class
import pickle,zstd
#from xmlrpc.server import SimpleXMLRPCServer
#from xmlrpc.server import SimpleXMLRPCRequestHandler
#logging.basicConfig(filename='server.log', level=logging.INFO)
log_formater = '%(threadName)s - %(asctime)s - %(levelname)s - %(lineno)d - %(message)s'

pathlib.Path("logs").mkdir(parents=True,exist_ok=True)
logging.basicConfig(filename='logs/server.log',filemode="w",level=logging.DEBUG, format=log_formater)

from configparser import ConfigParser
cfg = ConfigParser()

cfg.read('config/server.ini')
#conf
listen_addr = cfg.get('netowrk','listen_addr')
control_server_port = cfg.getint('netowrk','control_server_port')
control_server_addr = f"{listen_addr}:{control_server_port}"

data_server_port = cfg.getint('netowrk','data_server_port')

local_resource_dir = cfg.get('storage','local_resource_dir')
recive_dst_path = cfg.get('storage','recive_dst_path')

compress_type = cfg.get('compress','type')
MAX_MESSAGE_LENGTH = 150 * 1024 * 1024
rpc_options = [
    ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)
] 
max_rpc_workers=30
#MAX_MESSAGE_LENGTH = 150 * 1024 * 1024
def check_and_update(path,pos,data,open_file):
    global manager
    file = pathlib.Path(path)
    if not file.exists():
        file.touch()
    if path not in open_file:
        open_file[path] = manager.Lock()
    file_lock = open_file[path]
    with file_lock:
        with open(path, 'r+b') as f:
            f.seek(pos)
            f.write(data)
    return True

def gen_path(raw_path):
    if ":" in raw_path:
        raw_path = pathlib.PureWindowsPath(raw_path)
    else:
        raw_path = pathlib.PurePosixPath(raw_path)
    new_dir = pathlib.Path(recive_dst_path) / raw_path.parent.name
    dst_path = new_dir / raw_path.name
    pathlib.Path(new_dir).mkdir(parents=True,exist_ok=True)
    return dst_path

def request_dict(request):
    d = {
        "path": request.path,
        "total_size": request.total_size,
        "start_pos": request.start_pos,
        "length": request.length,
        "hash": request.hash,
        "hash_type": request.hash_type,
        "mtime": request.mtime
    }
    return d
class FileServiceServicer(data_check_pb2_grpc.FileServiceServicer):
    def CheckFile(self, request, context):
        global upload_id
        logging.debug(f"{request.path}-{request.start_pos} check receive")
        result = local_file_segment.get(request.hash,None)
        newpath = gen_path(request.path)
        
        if result:
            logging.debug(f"{request.path}-{request.start_pos} check resource found in local:{result}")
#            logging.debug(f"dest path:{newpath}")
            try:
                with open(result.path,"rb") as f:
                    f.seek(result.start_pos)
                    data = f.read(result.length)
                if not data:
                    raise Exception("data empyt")
                read_data_hash = hashlib.md5(data).hexdigest()
                if read_data_hash != request.hash:
                    raise Exception(f"data changed:{read_data_hash}")
                check_and_update(newpath,request.start_pos,data,open_file)
                logging.debug("{request.path}-{request.start_pos} check resource found in local:{result}")
                return data_check_pb2.FileCheckResponse(exist=True,upload_id = 0)
            except Exception as err:
                logging.error(f"local resoure err:{err}",exc_info=True)
#                with upload_info_lock:
                with upload_id.get_lock():
                    upload_id.value +=  1 
                now_upload_id = int(upload_id.value)
                upload_info[now_upload_id] = request.__dict__
                return data_check_pb2.FileCheckResponse(exist=False,upload_id=now_upload_id)
        else:
            with upload_id.get_lock():
                upload_id.value +=  1 
            now_upload_id = int(upload_id.value)
            upload_info[now_upload_id] = request_dict(request)
            return data_check_pb2.FileCheckResponse(exist=False,upload_id=now_upload_id)

        

    def UploadFile(self, request, context):
        npath = gen_path(request.path)
#        decompressed_data = lz4.frame.decompress(request.data)
        logging.debug(f"upload request:{request.path,request.length}")
        try:
            check_and_update(npath,request.start_pos,request.data,open_file)

            if request.hash not in local_file_segment:
                local_file_segment[request.hash] = transform_data_class.FileFragment(
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

def grpc_server(server_addr):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_rpc_workers),options = rpc_options)
    data_check_pb2_grpc.add_FileServiceServicer_to_server(FileServiceServicer(), server)
    server.add_insecure_port(server_addr)
    server.start()
    server.wait_for_termination()

def CheckFile(request):
    global upload_id
    logging.debug(f"uploadid:{upload_id}")
    result = local_file_segment.get(request.hash,None)
    newpath = gen_path(request.path)
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
            check_and_update(newpath,request.start_pos,data,open_file)
            return {"exist":True,"upload_id":0}
        except Exception as err:
            logging.error(f"local resoure err:{err}",exc_info=True)
            with upload_id.get_lock():
                now_upload_id = upload_id.value +  1 
                upload_id.value = now_upload_id
            upload_info[now_upload_id] = request.__dict__
            return {"exist":False,"upload_id":now_upload_id}
    else:
        with upload_id.get_lock():
            upload_id.value +=  1 
        now_upload_id = upload_id.value
        upload_info[upload_id] = request.__dict__
        return {"exist":False,"upload_id":now_upload_id}
    
#class RequestHandler(SimpleXMLRPCRequestHandler):
#    rpc_paths = ('/chunk_check',)

# Create server
#with SimpleXMLRPCServer(('localhost', 8000),
#                        requestHandler=RequestHandler) as server:
#    server.register_introspection_functions()

    # Register pow() function; this will use the value of
    # pow.__name__ as the name, which is just 'pow'.
#    server.register_function(CheckFile)

class DataHanlder(socketserver.BaseRequestHandler):
    def handle(self):
        id  = 0
        data = b""
        try:
            packet = self.request.recv(8)
            id = int.from_bytes(packet[:4], 'big')
            length = int.from_bytes(packet[4:], 'big')
            logging.debug(f"header id:{id} len:{length}")
            while True:
                packet = self.request.recv(MAX_MESSAGE_LENGTH)
                if not packet:
                    break
                data += packet
                if len(data) == length:
                    break
            

            seg_upload_info = upload_info.get(id,None)
            if seg_upload_info:
                #decompressed_data
                if compress_type == "zstd":
                    decompressed_data = zstd.decompress(data)
                else:
                    decompressed_data = data
                npath = gen_path(seg_upload_info["path"])
                check_and_update(npath,seg_upload_info["start_pos"],decompressed_data,open_file)

                request = seg_upload_info
                if request["hash"] not in local_file_segment:
                    local_file_segment[request["hash"]] = transform_data_class.FileFragment(
                        path=npath,
                        start_pos=request["start_pos"],
                        length = request["length"],
                        total_size = request["total_size"],
                        mtime = request["mtime"],
                        hash_type = request["hash_type"],
                        hash  = request["hash"]
                    )
                response = transform_data_class.upload_response(id=id,success=True)
                response = pickle.dumps(response)
                self.request.sendall(response)
            else:
                raise Exception(f"upload info not found{id}")
            
        except Exception as err:
            logging.error(f"upload failed:{id}" ,exc_info=True)
            try:
                response = transform_data_class.upload_response(id,False)
                response = pickle.dumps(response)
                self.request.sendall(response)
            except Exception as err:
                logging.error(f"upload send error failed:{err}",exc_info=True)
def tcp_server(server_port):
    with socketserver.ForkingTCPServer(("0.0.0.0", server_port), DataHanlder) as server:
        server.serve_forever()


class threading_global_data:
    def __init__(self) -> None:
        self.data = {}
        self.lock = threading.Lock()


if __name__ == '__main__':
    #TO DO:each file lock
#    file_lock = multiprocessing.Lock()
    #info lock
#    info_lock = threading.Lock()
    #
#    upload_info_lock = multiprocessing.Lock()
#    upload_id_lock = threading.Lock()
#    upload_info = {}
#    upload_id = 0

    print("program is starting....")
    local_file_segment = storge_cdc.scan_directory(local_resource_dir)
    local_file_segment_dict = {x.hash:x for x in local_file_segment}
    manager = multiprocessing.Manager()
    local_file_segment = manager.dict(local_file_segment_dict)
    open_file = manager.dict()
    upload_info =  manager.dict()
    upload_id = multiprocessing.Value("i",0)
    upload_id.value
    logging.info(f"init local {len(local_file_segment)} file segments")

    print(f"init local {len(local_file_segment)} file segments")
#    grpc_server(server_addr)
    grpc_thread  = threading.Thread(target=grpc_server,args=(control_server_addr,))
    tcp_thread  = threading.Thread(target=tcp_server,args=(data_server_port,))
    tcp_thread.start()
    grpc_thread.start()

    tcp_thread.join()
    grpc_thread.join()


