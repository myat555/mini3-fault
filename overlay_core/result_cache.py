import json
import threading
import time
from typing import Dict, List, Optional


class ChunkedResult:
    """Stores materialized query output and exposes chunk level accessors."""

    def __init__(
        self,
        uid: str,
        records: List[Dict[str, object]],
        chunk_size: int,
        ttl_seconds: int,
        metadata: Optional[Dict[str, object]] = None,
    ):
        self.uid = uid
        self.records = list(records)
        self.chunk_size = max(1, chunk_size)
        self.ttl_seconds = ttl_seconds
        self.metadata = metadata or {}
        self.created_at = time.time()
        self.expires_at = self.created_at + ttl_seconds
        self.total_records = len(self.records)
        self.total_chunks = self._compute_total_chunks()

    def _compute_total_chunks(self) -> int:
        if self.total_records == 0:
            return 1
        return (self.total_records + self.chunk_size - 1) // self.chunk_size

    def is_expired(self) -> bool:
        return time.time() >= self.expires_at

    def get_chunk(self, index: int) -> Optional[Dict[str, object]]:
        if index < 0 or index >= self.total_chunks:
            return None

        start = index * self.chunk_size
        end = min(start + self.chunk_size, self.total_records)
        payload = self.records[start:end]
        return {
            "uid": self.uid,
            "chunk_index": index,
            "total_chunks": self.total_chunks,
            "data": payload,
            "is_last": index == self.total_chunks - 1,
        }


class ResultCache:
    """Thread-safe cache with TTL eviction for chunked query results."""

    def __init__(self, ttl_seconds: int = 180):
        self.ttl = ttl_seconds
        self._lock = threading.Lock()
        self._store: Dict[str, ChunkedResult] = {}

    def store(self, result: ChunkedResult) -> None:
        with self._lock:
            self._store[result.uid] = result
            self._purge_locked()

    def get(self, uid: str) -> Optional[ChunkedResult]:
        with self._lock:
            result = self._store.get(uid)
            if result and not result.is_expired():
                return result
            if result:
                del self._store[uid]
            return None

    def delete(self, uid: str) -> None:
        with self._lock:
            self._store.pop(uid, None)

    def __len__(self) -> int:
        with self._lock:
            self._purge_locked()
            return len(self._store)

    def _purge_locked(self) -> None:
        expired = [uid for uid, result in self._store.items() if result.is_expired()]
        for uid in expired:
            del self._store[uid]

