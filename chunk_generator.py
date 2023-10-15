import abc
#from typing import Any, Dict, AsyncGenerator
import multiprocessing
from dataclasses import dataclass
import os
import fastcdc
from dataclasses import dataclass
from typing import Optional

@dataclass
class file_chunk:
    offset: int
    length: int
    data: bytes
    hash: str
@dataclass
class FileFragment:
    path: str
    start_pos: int
    length: int
    total_size: int
    mtime: int
    data: Optional[bytes] = None
    hash_type: Optional[str] = None
    hash: Optional[str] = None

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
        for result in results:
#            with self.lock:
            r =  FileFragment(
                path = path,
                start_pos = result.offset,
                length = result.length,
                total_size=size,
                mtime = int(os.path.getmtime(path)),
                hash = result.hash,
                hash_type =  "md5",
                data = result.data
            )
            yield r