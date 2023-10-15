import logging,multiprocessing,threading
#from multiprocessing import pool
from functools import partial
from  upload_client import grpc_tcp_client as upload_client
from chunk_generator import cdc_chunk as chunk_generator
import hashlib,os,time
import transform_data_class
from multiprocessing.pool import ThreadPool

log_formater = '%(threadName)s - %(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
logging.basicConfig(filename='logs/client.log',filemode="w",level=logging.DEBUG, format=log_formater)

chunk_size = 1024 * 1024 * 128
hash_filter = hashlib.md5
server_addr = "127.0.0.1"
server_port = 50051
data_port = 50052
MAX_UPLOAD_FILES = 2
MAX_UPLOAD_SEGMENGT = 4


import multiprocessing.pool

class NoDaemonProcess(multiprocessing.Process):
    @property
    def daemon(self):
        return False

    @daemon.setter
    def daemon(self, value):
        pass


class NoDaemonContext(type(multiprocessing.get_context())):
    Process = NoDaemonProcess

# We sub-class multiprocessing.pool.Pool instead of multiprocessing.Pool
# because the latter is only a wrapper function, not a proper class.
class NestablePool(multiprocessing.pool.Pool):
    def __init__(self, *args, **kwargs):
        kwargs['context'] = NoDaemonContext()
        super(NestablePool, self).__init__(*args, **kwargs)


def gen_check_request(seg):
    seg_dict = seg.__dict__
    check_request_d  = seg_dict.copy()
    del check_request_d["data"]
    check_request = transform_data_class.check_request(**check_request_d)
    return check_request

def gen_upload_request(upload_id,data):
    return transform_data_class.upload_request(id=upload_id,data=data)
#    pass

def segment_process(seg):
    global before_split
    split_time = time.time()
    logging.debug(f"make seg {seg.start_pos} cost_time:{split_time - before_split}")
    client = upload_client(server_addr,server_port,data_port)
    check_request = gen_check_request(seg)
    check_result = client.check(check_request)
    upload_after = time.time()
    logging.info(f"check_result:{check_result},cost_time:{upload_after - split_time}")
    if check_result.exist:
        return
    else:
        upload_request = gen_upload_request(check_result.upload_id,seg.data)
        before_upload = time.time()
        response = client.upload(upload_request)
        after_upload = time.time()
        logging.info(f"upload resonse:{response},cost_time:{after_upload - before_upload}")
def process_file(path):
    chunk_generator_ins = chunk_generator(hf = hash_filter,chunk_size = chunk_size)
    file_segments = chunk_generator_ins.chunk_iter(path)
    seg_pool = multiprocessing.Pool(MAX_UPLOAD_SEGMENGT)
    global before_split
    before_split = time.time()
    for f in file_segments:
        segment_process(f)
#    seg_pool.map(segment_process,file_segments)
    
#    for s in file_segments:
#        segment_process(s)
    return

def get_file_list():
#    pass
    return ["/opt/vmTransfer/dataTransfer/python/py-datasync/test/data/test-img/win7 sp1 旗舰版 2020.05 x64.iso"]



def run():
    file_list = get_file_list()
    logging.debug(f"upload file list:{file_list}")
    for x in file_list:
        process_file(x)
#    file_pool = NestablePool(MAX_UPLOAD_FILES)
#    file_pool.map(process_file,file_list)
    logging.debug(f"Done!")



if __name__ == '__main__':
    before_split = 0.0
    run()