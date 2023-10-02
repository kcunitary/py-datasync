import os
import json
from multiprocessing import Pool, cpu_count

chunk_size=40*1024*1024

class file_segment:
    def __init__(self,path,hash_type,hash,start_pos,end_pos,file_fullsize,mtime) -> None:
        self.path
        pass

import json
import time
from dataclasses import dataclass
from typing import Optional

@dataclass
class FileFragment:
    path: str
    start_pos: int
    length: int
    total_size: int
    mtime: time.struct_time
    hash_type: Optional[str] = None
    hash: Optional[str] = None

    def to_json(self):
        return json.dumps(self.__dict__, default=str)


def get_file_list(directory):
    files = []
    for foldername, subfolders, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(foldername, filename)
            files.append(file_path)
    return files

def process_file_list(files,process_file):
    pool = Pool(cpu_count())
    results = pool.map(process_file, files)
    pool.close()
    pool.join()
    return [item for sublist in results if sublist for item in sublist]

def scan_directory(directory,fn):
    files = get_file_list(directory)
    return process_file_list(files,fn)

def save_results(results, filename):
    with open(filename, 'w') as f:
        json.dump(results, f)

def load_results(filename):
    with open(filename, 'r') as f:
        return json.load(f)





