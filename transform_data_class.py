from dataclasses import dataclass
import json
import time
from dataclasses import dataclass
from typing import Optional
@dataclass
class check_request:
    path: str
    start_pos: int
    length: int
    total_size: int
    mtime: int
    hash_type: Optional[str] = None
    hash: Optional[str] = None


@dataclass
class upload_response:
    id: int
    success: bool

@dataclass
class upload_request:
    id: int
    data: bytes