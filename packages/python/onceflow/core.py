import time
from typing import Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod

class ConflictError(Exception):
    pass

@dataclass
class Result:
    status: str
    status_code: int
    headers: str
    body: str
    created_at: int

class Storage(ABC):
    @abstractmethod
    def acquire(self, key: str, lock_ttl: int) -> Optional[Result]:
        pass

    @abstractmethod
    def resolve(self, key: str, res: Result, retention_ttl: int) -> None:
        pass

    @abstractmethod
    def fail(self, key: str) -> None:
        pass

class Engine:
    def __init__(self, storage: Storage, lock_ttl_ms: int = 30000, retention_ttl_ms: int = 86400000):
        self.storage = storage
        self.lock_ttl = lock_ttl_ms
        self.retention_ttl = retention_ttl_ms

    def acquire(self, key: str) -> Optional[Result]:
        return self.storage.acquire(key, self.lock_ttl)

    def resolve(self, key: str, res: Result) -> None:
        self.storage.resolve(key, res, self.retention_ttl)

    def fail(self, key: str) -> None:
        self.storage.fail(key)
