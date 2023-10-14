import grpc
import data_check_pb2
import data_check_pb2_grpc
import storge_cdc,local_storage_scan
import time,socket
import os,logging
import lz4.frame
from functools import partial
import multiprocessing
log_formater = '%(threadName)s - %(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
logging.basicConfig(filename='logs/client.log',filemode="w",level=logging.DEBUG, format=log_formater)

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
chunk_size = 64 *1024*1024
tcp_port = 50052
tcp_addr = "127.0.0.1"
def sizeof_fmt(num, suffix="B"):
    for unit in ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"):
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


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
#    data_compress = zlib.compress(data,3)
    data_compress = lz4.frame.compress(data,block_size=lz4.frame.BLOCKSIZE_MAX4MB)
    r = data_check_pb2.FileUploadRequest(path = f.path,
    name = os.path.basename(f.path),
    total_size = f.total_size,
    start_pos = f.start_pos,
    length = f.length,
    hash =f.hash,
    hash_type = f.hash_type,
    mtime = int(f.mtime),
    compress_type = "lz4",
    data = data_compress
    )
    return r

def process_seg(seg,stub):
        time_start = time.time()
        request = info_2_check_request(seg)
        logging.debug(f"{seg.path}:{seg.start_pos}-{sizeof_fmt(seg.length)} check prepared")
#        with uploader_locker:
        response = stub.CheckFile(request)
        logging.debug(f"{seg.path}:{seg.start_pos}-{sizeof_fmt(seg.length)} check response:{response}")
        if response.exists:
            logging.info(f"{seg.path}:{seg.start_pos}-{sizeof_fmt(seg.length)} server has resource:succeed!")
            pass
        else:
            request = info_2_upload_request(seg)
            logging.debug(f"{seg.path}:{seg.start_pos}-{sizeof_fmt(seg.length)} upload prepared")
#            with uploader_locker:
            response = stub.UploadFile(request)
            logging.debug(f"{seg.path}:{seg.start_pos}-{sizeof_fmt(seg.length)} upload response:{response.success}")
        delta = time.time() - time_start
        logging.info(f"transfer size:{sizeof_fmt(seg.length)} time:{delta}")

def tcp_upload(ip,port,id,filename,offset,length):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect((ip, port))
    with open(filename, 'rb') as f:
        f.seek(offset)
        chunk = f.read(length)
        compressed_chunk = lz4.frame.compress(chunk,block_size=lz4.frame.BLOCKSIZE_MAX4MB)
        header  = id.to_bytes(4, 'big')
        s_1 = client_socket.sendall(header)
        s_2 = client_socket.sendall(compressed_chunk)
    client_socket.close()
    return any([s_1,s_2])
    
def process_seg_per_client(seg,addr):
    time_start = time.time()
    channel = grpc.insecure_channel(addr,options=rpc_options)
    stub = data_check_pb2_grpc.FileServiceStub(channel)
    request = info_2_check_request(seg)
    logging.debug(f"{seg.path}:{seg.start_pos}-{sizeof_fmt(seg.length)} check prepared")
    response = stub.CheckFile(request)
    logging.debug(f"{seg.path}:{seg.start_pos}-{sizeof_fmt(seg.length)} check response:{response}")
    if response.upload_id == 0:
        logging.info(f"{seg.path}:{seg.start_pos}-{sizeof_fmt(seg.length)} server has resource:succeed!")
        pass
    else:
        upload_bytes = tcp_upload(tcp_addr,tcp_port,response.upload_id,seg.path,seg.start_pos,seg.length)
        logging.debug(f"{seg.path}:{seg.start_pos}-{sizeof_fmt(seg.length)} upload {upload_bytes}")
    delta = time.time() - time_start
    logging.info(f"transfer size:{sizeof_fmt(seg.length)} time:{delta} speed:{sizeof_fmt(seg.length/delta)}/s")
def process_file(path):
    file_segments = storge_cdc.process_file(path)
    process_seg_partial = partial(process_seg_per_client, addr=server_addr)
    seg_pool = multiprocessing.Pool()
    return seg_pool.map(process_seg_partial,file_segments)

def run():
#    file_list = storge_rsync.get_file_list(dir_to_send)
    file_list = ["/opt/vmTransfer/dataTransfer/python/py-datasync/test/data/test-img/win7 sp1 旗舰版 2020.05 x64.iso"]
    for f in file_list:
        process_file(f)



if __name__ == '__main__':
    run()
