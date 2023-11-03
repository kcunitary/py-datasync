import logging,threading
import pathlib
from  src.client.upload_client import grpc_tcp_client as upload_client
from src.client.chunk_generator import cdc_chunk as chunk_generator
import hashlib,os,time
import subprocess
import src.common.dataclass.transform_data_class as transform_data_class
from src.common.unit import sizeof_fmt
from configparser import ConfigParser
import click
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

pathlib.Path("logs").mkdir(parents=True,exist_ok=True)

log_formater = '%(threadName)s - %(asctime)s - %(levelname)s - %(lineno)d - %(message)s'

logging.basicConfig(filename='client.log',filemode="w",level=logging.DEBUG, format=log_formater)


import multiprocessing.pool

def printProgress(iteration, total, prefix='', suffix='', decimals=1, barLength=100):
    """
    Call in a loop to create a terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        barLength   - Optional  : character length of bar (Int)
    """
    import sys
    formatStr = "{0:." + str(decimals) + "f}"
    percent = formatStr.format(100 * (iteration / float(total)))
    filledLength = int(round(barLength * iteration / float(total)))
    bar = '#' * filledLength + '-' * (barLength - filledLength)
    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percent, '%', suffix)),
    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()

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
    client = upload_client(server_addr,server_port,data_port)
    check_request = gen_check_request(seg)
    check_result = client.check(check_request)
    upload_after = time.time()
    p = None
    logging.info(f"segment:{seg.path}-{seg.start_pos} check_result:{check_result} cost_time:{upload_after - split_time}")
    if check_result.exist:
        logging.debug(f"segment:{seg.path}-{seg.start_pos} skip")
        after_upload = time.time()
    else:
        send_data = seg.data
        compress_before = time.time()
        if compress_type == "zstd":
            import zstd
            send_data = zstd.compress(send_data,compress_level)
        elif compress_type == "lz4":
            import lz4.frame
            send_data = lz4.frame.compress(send_data,compression_level=compress_level)
        compress_after = time.time()
        upload_request = gen_upload_request(check_result.upload_id,send_data)
        logging.debug(f"segment:{seg.path}-{seg.start_pos} compress cost:{compress_after - compress_before}")
        before_upload = time.time()
        response = client.upload(upload_request)
        after_upload = time.time()
        logging.info(f"upload resonse:{response},cost_time:{after_upload - before_upload}")
#    printProgress(seg.start_pos + seg.length)
    printProgress(seg.start_pos + seg.length, seg.total_size, prefix=f'{seg.path}:', suffix='Complete', barLength=50)
    logging.info(f"seg final: size:{sizeof_fmt(seg.length)}  cost_time:{after_upload - split_time}  speed:{sizeof_fmt(seg.length/(after_upload - split_time))}/s")
    return p
def segment_process_sem(seg,sem):
    with sem:
        p = threading.Thread(target=segment_process,args=(seg,))
        p.start()
    return p

def process_file(path):
    #split one file to segments
    chunk_generator_ins = chunk_generator(hf = hash_filter,chunk_size = chunk_size)
    file_segments = chunk_generator_ins.chunk_iter(path)
    before_file = time.time()
    thread_jobs = []
    sem = threading.Semaphore(3)
    for f in file_segments:
        p = segment_process_sem(f,sem)
        thread_jobs.append(p)
    for p in thread_jobs:
        if p:
            p.join()
    file_size = os.path.getsize(path)
    cost = time.time() - before_file
    logging.debug(f"file:{path} size:{file_size} cost:{cost} speed:{sizeof_fmt(file_size/cost)} /s")
    return

def get_file_list(path):
    return [os.path.join(root, file) for root, dirs, files in os.walk(path) for file in files if file.endswith("vhdx")]
def convert(path):
    current_dir = os.getcwd() # 获取当前目录的路径
    exe_file = os.path.join(current_dir, "qemu-img","qemu-img.exe") # 拼接exe程序的文件名
    dst_path = path + ".raw"
    subprocess.run([exe_file,"convert -f vhdx -O raw",path,dst_path]) # 执行exe程序
    return dst_path

@click.command()
@click.argument("path", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option(
    "-c", "--convert", help="convert before transfer.", is_flag=True,
)
def run(path,convert):
    file_list = get_file_list(path)
    logging.debug(f"upload file list:{file_list}")
    jobs = []
    for x in file_list:
#        process_file(x)
        p = threading.Thread(target=process_file,args=(x,))
        p.start()
        jobs.append(p)
    for j in jobs:
        j.join()
    logging.debug(f"Done!")



if __name__ == '__main__':
    run()