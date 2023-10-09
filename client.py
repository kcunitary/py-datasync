import grpc
import data_check_pb2
import data_check_pb2_grpc
import storge_cdc,local_storage_scan
import hashlib,time
import os,zlib,logging
MAX_MESSAGE_LENGTH = 1024 * 1024 * 1024
options = [
    ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
    ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH)
]

log_formater = '%(threadName)s - %(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
logging.basicConfig(filename='client.log',level=logging.DEBUG, format=log_formater)
def scan_dir_vhdx(dirs):
    fn = None
#    fn = lambda x:x.endswith("vhdx")
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

def run(d="/tmp/testdata"):
    file_segment_list = scan_dir_vhdx(d)
    logging.info(f"scan {len(file_segment_list)} file segments")
    channel = grpc.insecure_channel('localhost:50051',options=options)
    stub = data_check_pb2_grpc.FileServiceStub(channel)
    for seg in file_segment_list:
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
    channel.close()
def test():
    r = scan_dir_vhdx("/tmp/testdata/")
    print(r)
if __name__ == '__main__':
    run()
