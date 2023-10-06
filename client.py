import grpc
import data_check_pb2
import data_check_pb2_grpc
import storge_cdc,local_storage_scan
import hashlib
import os,zlib,logging

logging.basicConfig(filename='client.log', level=logging.INFO)


def scan_dir_vhdx(dirs):
    fn = None
#    fn = lambda x:x.endswith("vhdx")
    return storge_cdc.scan_directory(dirs,fn)

def info_2_check_request(f):
    r = data_check_pb2.FileCheckRequest(path = f.path,
     name = os.path.basename(f.path),
     total_size = f.total_size,
     start_pos = f.start_pos,
     length = f.length,
     hash =f.hash,
     hash_type = f.hash_type,
     mtime = f.mtime)
    return r


def info_2_upload_request(f):
    with open(f.path,"rb") as f:
        f.seek(f.start_pos)
        data = f.read(f.length)
    if data:
        data_compress = zlib.compress(data,3)
        r = data_check_pb2.FileCheckRequest(path = f.path,
        name = os.path.basename(f.path),
        total_size = f.total_size,
        start_pos = f.start_pos,
        length = f.length,
        hash =f.hash,
        hash_type = f.hash_type,
        mtime = f.mtime,
        compress_type = "zlib",
        data = data_compress
        )
    return r

def run(d="/opt"):

    file_segment_list = scan_dir_vhdx(d)
    channel = grpc.insecure_channel('localhost:50051')
    stub = data_check_pb2_grpc.FileServiceStub(channel)
    for seg in file_segment_list:
        request = info_2_check_request(seg)
        response = stub.CheckFile(request)
        if response.exists:
            pass
        else:
            request = info_2_upload_request(seg)
            response = stub.UploadFile(request)
            print(response.success)
    channel.close()
if __name__ == '__main__':
    run()
