import fastcdc,logging
from functools import partial
from ..common.dataclass.transform_data_class import FileFragment

import hashlib,os
from . import local_storage_scan
def process_file(file_path,fat=True):
    try:
        seg_results = fastcdc.fastcdc(file_path,avg_size=128*1024*1024, fat=fat, hf=hashlib.md5)
        for result in seg_results:
            size = os.path.getsize(file_path)
            yield FileFragment(
                path = file_path,
                start_pos = result.offset,
                length = result.length,
                total_size=size,
                mtime = int(os.path.getmtime(file_path)),
                hash = result.hash,
                hash_type =  "md5",
                data = result.data
            )
    except Exception as e:
        logging.error(f'Error processing file {file_path}: {e}',exc_info=True)

def process_file_list(f):
    return tuple(process_file(f,fat=False))
def scan_directory(dirpath):
    return local_storage_scan.scan_directory(dirpath,process_file_list)


if __name__ == "__main__":
    seg_results = fastcdc.fastcdc("/opt/vmTransfer/dataTransfer/python/py-datasync/test/data/test-img/win7 sp1 旗舰版 2020.05 x64.iso",min_size=64*1024*1024, avg_size=100*1024*1024, max_size=800*1024*1024, fat=True, hf=hashlib.md5)
    seg_results = list(seg_results)
    print(len(seg_results[0].data))
    
    print(type(seg_results[0].data))