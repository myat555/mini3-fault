import statistics
import threading
import time
from collections import deque
from typing import Dict


class MetricsTracker:
    """Collects rolling statistics for the overlay node."""

    def __init__(self, window: int = 200):
        self._lock = threading.Lock()
        self._durations = deque(maxlen=window)
        self._completed = 0
        self._start = time.time()

    def record_completion(self, duration_ms: float) -> None:
        with self._lock:
            self._completed += 1
            self._durations.append(duration_ms)

    def snapshot(self) -> Dict[str, float]:
        with self._lock:
            avg = statistics.fmean(self._durations) if self._durations else 0.0
            uptime = time.time() - self._start
            rate = (self._completed / uptime) if uptime else 0.0
            return {
                "avg_ms": avg,
                "completed": self._completed,
                "uptime": uptime,
                "throughput": rate,
            }

