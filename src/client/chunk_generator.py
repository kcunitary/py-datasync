import abc
#from typing import Any, Dict, AsyncGenerator
import multiprocessing
import os
import fastcdc
from dataclasses import dataclass
from typing import Optional
from ..common.dataclass.transform_data_class import FileFragment
#/opt/vmTransfer/dataTransfer/python/py_datasync/src/common/dataclass/transform_data_class.py

""" @dataclass
class FileFragment:
    path: str
    start_pos: int
    length: int
    total_size: int
    mtime: int
    data: Optional[bytes] = None
    hash_type: Optional[str] = None
    hash: Optional[str] = None """

    
class chunk_genator(abc.ABC):
    def __init__(self, hf,chunk_size:int) -> None:
        self.hash = hf
        self.chunk_size = chunk_size

    def chunk_iter(self,path):
        pass

class cdc_chunk(chunk_genator):
    def __init__(self, hf,chunk_size:int) -> None:
        self.hash = hf
        self.chunk_size = chunk_size
        self.lock = multiprocessing.Lock()
    def chunk_iter(self,path):
        results =  fastcdc.fastcdc(path,avg_size=self.chunk_size, fat=True, hf=self.hash)
        size = os.path.getsize(path)
        mtime = int(os.path.getmtime(path))
        all = []
        for result in results:
            with self.lock:
                r =  FileFragment(
                    path = path,
                    start_pos = result.offset,
                    length = result.length,
                    total_size=size,
                    mtime = mtime,
                    hash = result.hash,
                    hash_type =  "md5",
                    data = result.data
                )
            yield r