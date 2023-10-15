import abc
from typing import Any, Dict, AsyncGenerator
from dataclasses import dataclass
import typing
@dataclass
class file_chunk:
    offset: int
    length: int
    data: bytes
    hash: str

class chunk_genator(abc.ABC):
    def __init__(self, hf:function,chunk_size:int) -> None:
        self.hash = hf
        self.chunk_size = chunk_size

    def chunk_iter(self) -> typing.Generator[file_chunk, None, None]:
        pass