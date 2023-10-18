import logging,multiprocessing,threading
#from multiprocessing import pool
import zstd
from  src.client.upload_client import grpc_tcp_client as upload_client
from src.client.chunk_generator import cdc_chunk as chunk_generator
import hashlib,os,time
import src.common.dataclass.transform_data_class as transform_data_class
from multiprocessing.pool import ThreadPool
from src.common.unit import sizeof_fmt
from configparser import ConfigParser
cfg = ConfigParser()

cfg.read('config/client.ini',encoding="UTF-8")
fils_list = cfg.get('files','send_list').split(",")
remote_addr = cfg.get('netowrk','server_add')
control_server_port = cfg.getint('netowrk','control_server_port')
control_server_addr = f"{remote_addr}:{control_server_port}"
compress_type = cfg.get('compress','type')
compress_level = cfg.getint('compress','level')
data_server_port = cfg.getint('netowrk','data_server_port')
chunk_size = 1024 * 1024 * 128
hash_filter = hashlib.md5
server_addr = remote_addr
server_port = control_server_port
data_port = data_server_port
MAX_UPLOAD_FILES = 2
MAX_UPLOAD_SEGMENGT = 4


log_formater = '%(threadName)s - %(asctime)s - %(levelname)s - %(lineno)d - %(message)s'
logging.basicConfig(filename='logs/client.log',filemode="w",level=logging.DEBUG, format=log_formater)



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
    split_time = time.time()
#    logging.info(f"segment:{seg.path}-{seg.start_pos} start progress")
    client = upload_client(server_addr,server_port,data_port)
    check_request = gen_check_request(seg)
    check_result = client.check(check_request)
    upload_after = time.time()
    logging.info(f"segment:{seg.path}-{seg.start_pos} check_result:{check_result} cost_time:{upload_after - split_time}")
    if check_result.exist:
        logging.debug(f"segment:{seg.path}-{seg.start_pos} skip")
        return
    else:
        if compress_type == "zstd":
            compress_before = time.time()
            compressed_data = zstd.compress(seg.data,compress_level)
            compress_after = time.time()
            upload_request = gen_upload_request(check_result.upload_id,compressed_data)
            logging.debug(f"segment:{seg.path}-{seg.start_pos} compress cost:{compress_after - compress_before}")
        else:
            upload_request = gen_upload_request(check_result.upload_id,seg.data)
        before_upload = time.time()
        response = client.upload(upload_request)
        after_upload = time.time()
        logging.info(f"upload resonse:{response},cost_time:{after_upload - before_upload}")
    logging.info(f"seg final: size:{sizeof_fmt(seg.length)}  cost_time:{after_upload - split_time}  speed:{sizeof_fmt(seg.length/(after_upload - split_time))}/s")


def process_file(path):
    #split one file to segments
    chunk_generator_ins = chunk_generator(hf = hash_filter,chunk_size = chunk_size)
    file_segments = chunk_generator_ins.chunk_iter(path)
    before_file = time.time()

#    seg_pool = multiprocessing.Pool(MAX_UPLOAD_SEGMENGT)
#    seg_pool.map(segment_process,file_segments)
    for f in file_segments:
        segment_process(f)
    file_size = os.path.getsize(path)
    cost = time.time() -before_file
    logging.debug(f"file:{path} size:{file_size} cost:{cost} speed:{sizeof_fmt(file_size/cost)} /s")
    return

def get_file_list():
#    pass
    return fils_list



def run():
    file_list = get_file_list()
    logging.debug(f"upload file list:{file_list}")
    for x in file_list:
        process_file(x)
#    file_pool = NestablePool(MAX_UPLOAD_FILES)
#    file_pool.map(process_file,file_list)
    logging.debug(f"Done!")



if __name__ == '__main__':
    run()