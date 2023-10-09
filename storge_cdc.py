import fastcdc,json
from functools import partial

import hashlib,os
import local_storage_scan as local_storage_scan
def process_file(file_path):
    try:
        seg_results = fastcdc.fastcdc(file_path,min_size=10*1024*1024, avg_size=40*1024*1024, max_size=200*1024*1024, fat=True, hf=hashlib.md5)
        all = []
        for result in seg_results:
            end = result.offset +  result.length
            size = os.path.getsize(file_path)

            item = local_storage_scan.FileFragment(
                path = file_path,
                start_pos = result.offset,
                length = result.length,
                total_size=size,
                mtime = os.path.getmtime(file_path),
                hash = result.hash,
                hash_type =  "md5"
            )
            all.append(item)
        return all
    except Exception as e:
        print(f'Error processing file {file_path}: {e}')

def scan_directory(dirpath):
    return local_storage_scan.scan_directory(dirpath,process_file)
