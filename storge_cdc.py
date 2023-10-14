import fastcdc,logging
from functools import partial

import hashlib,os
import local_storage_scan as local_storage_scan
def process_file(file_path):
    try:
        seg_results = fastcdc.fastcdc(file_path,min_size=64*1024*1024, avg_size=100*1024*1024, max_size=800*1024*1024, fat=True, hf=hashlib.md5)
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
        logging.error(f'Error processing file {file_path}: {e}',exc_info=True)

def scan_directory(dirpath):
    return local_storage_scan.scan_directory(dirpath,process_file)
