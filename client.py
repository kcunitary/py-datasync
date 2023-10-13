import grpc
import data_check_pb2
import data_check_pb2_grpc
import storge_cdc,local_storage_scan
import hashlib,time
import os,zlib,logging
from threading import  Lock
from functools import partial
from concurrent.futures import ThreadPoolExecutor
import storge_rsync
log_formater = '%(threadName)s - %(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
logging.basicConfig(filename='client.log',level=logging.DEBUG, format=log_formater)

MAX_MESSAGE_LENGTH = 150 * 1024 * 1024
""" rpc_options = [
  #  ('grpc.max_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)
] """
rpc_options = [
    ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)
]
server_addr = '[::]:50051'
dir_to_send = "/tmp/testdata"

def scan_dir_vhdx(dirs):
    return storge_cdc.scan_directory(dirs)

def info_2_check_request(f):
    r = data_check_pb2.FileCheckRequest(path = f.path,
     name = os.path.basename(f.path),
     total_size = f.total_size,
     start_pos = f.start_pos,
     length = f.length,
     hash =f.hash,
     hash_type = f.hash_type,
     mtime = int(f.mtime))
    return r


def info_2_upload_request(f):
    with open(f.path,"rb") as f2:
        f2.seek(f.start_pos)
        data = f2.read(f.length)
 #   if data:
    data_compress = zlib.compress(data,3)
    r = data_check_pb2.FileUploadRequest(path = f.path,
    name = os.path.basename(f.path),
    total_size = f.total_size,
    start_pos = f.start_pos,
    length = f.length,
    hash =f.hash,
    hash_type = f.hash_type,
    mtime = int(f.mtime),
    compress_type = "zlib",
    data = data_compress
    )
    return r

def process_seg(seg,stub):
        time_start = time.time()
        request = info_2_check_request(seg)
        logging.debug(f"request:{request}")
#        with uploader_locker:
        response = stub.CheckFile(request)
        logging.debug(f"{seg.path} check response:{response}")
        if response.exists:
            logging.info("server has resource:succeed!")
            pass
        else:
            request = info_2_upload_request(seg)
            logging.debug(f"upload request:{request.path,request.length}")
#            with uploader_locker:
            response = stub.UploadFile(request)
            logging.debug(f"upload response:{response.success}")
        delta = time.time() - time_start
        logging.info(f"transfer size:{seg.total_size} time:{delta}")

    
def run():
    channel = grpc.insecure_channel('127.0.0.1:50051',options=rpc_options)
    stub = data_check_pb2_grpc.FileServiceStub(channel)
    
#    file_list = storge_rsync.get_file_list(dir_to_send)
    file_list = ["/opt/vmTransfer/dataTransfer/python/py-datasync/test/data/test-img/win7 sp1 旗舰版 2020.05 x64.iso"]
    process_seg_partial = partial(process_seg, stub=stub)
    for file in file_list:
        file_segments = storge_cdc.process_file(file)
        for seg in file_segments:
            process_seg_partial(seg)
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(process_seg_partial, file_segments)
    channel.close()
    
 
    """
     with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(process_seg_partial, file_segment_list)
    """
    """
    for seg in file_segment_list:
        time_start = time.time()
        request = info_2_check_request(seg)
        logging.debug(f"request:{request}")
        response = stub.CheckFile(request)
        logging.debug(f"{seg.path} check response:{response}")
        if response.exists:
            logging.info("server has resource:succeed!")
            pass
        else:
            request = info_2_upload_request(seg)
            logging.debug(f"upload request:{request.path,request.length}")
            response = stub.UploadFile(request)
            logging.debug(f"upload response:{response.success}")
        delta = time.time() - time_start
        logging.info(f"transfer size:{seg.total_size} time:{delta}")
    """
    channel.close()

if __name__ == '__main__':
    run()
