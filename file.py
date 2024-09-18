# file.py

from typing import List
import time

class FileVersion:
    def __init__(self, content: str, timestamp: float, version: int):
        self.content = content
        self.timestamp = timestamp
        self.version = version

class File:
    def __init__(self, filename: str, owner_server_id: str):
        self.filename = filename
        self.owner_server_id = owner_server_id
        self.versions: List[FileVersion] = []
        self.lease = None  # Lease object

    def add_version(self, content: str):
        version_number = len(self.versions) + 1
        new_version = FileVersion(content, time.time(), version_number)
        self.versions.append(new_version)

    def get_latest_version(self) -> FileVersion:
        if self.versions:
            return self.versions[-1]
        else:
            return None

class Lease:
    def __init__(self, lessee: str, expiry_time: float):
        self.lessee = lessee
        self.expiry_time = expiry_time

    def is_expired(self) -> bool:
        return time.time() > self.expiry_time
