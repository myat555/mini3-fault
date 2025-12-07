import time
import random
import functools
from typing import Callable, Type, Tuple, Optional

class RetryWithBackoff:
    """
    Retry logic with exponential backoff and jitter.
    """
    def __init__(
        self,
        max_retries: int = 3,
        initial_delay: float = 0.1,
        max_delay: float = 2.0,
        backoff_factor: float = 2.0,
        jitter: bool = True,
        exceptions: Tuple[Type[Exception], ...] = (Exception,)
    ):
        self.max_retries = max_retries
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_factor = backoff_factor
        self.jitter = jitter
        self.exceptions = exceptions

    def __call__(self, func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = self.initial_delay
            last_exception = None
            
            for attempt in range(self.max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except self.exceptions as e:
                    last_exception = e
                    if attempt == self.max_retries:
                        break
                    
                    # Calculate wait time
                    wait = delay
                    if self.jitter:
                        wait *= (0.5 + random.random())
                    
                    # Cap at max_delay
                    wait = min(wait, self.max_delay)
                    
                    time.sleep(wait)
                    
                    # Increase delay for next attempt
                    delay *= self.backoff_factor
            
            raise last_exception
        return wrapper

class CircuitBreaker:
    """
    Circuit breaker pattern to prevent cascading failures.
    """
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 10.0,
        exceptions: Tuple[Type[Exception], ...] = (Exception,)
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.exceptions = exceptions
        
        self._failures = 0
        self._last_failure_time = 0
        self._state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self._lock = functools.wraps(lambda: None)(lambda: None) # Dummy lock, use threading.Lock in real usage if needed
        # Actually, let's use a real lock if we are going to be thread safe
        import threading
        self._lock = threading.Lock()

    def __call__(self, func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            if not self.allow_request():
                raise Exception("CircuitBreaker is OPEN")
            
            try:
                result = func(*args, **kwargs)
                self.record_success()
                return result
            except self.exceptions as e:
                self.record_failure()
                raise e
        return wrapper

    def allow_request(self) -> bool:
        with self._lock:
            if self._state == "OPEN":
                if time.time() - self._last_failure_time > self.recovery_timeout:
                    self._state = "HALF_OPEN"
                    return True
                return False
            return True

    def record_success(self):
        with self._lock:
            if self._state == "HALF_OPEN":
                self._state = "CLOSED"
                self._failures = 0
            elif self._state == "CLOSED":
                self._failures = 0

    def record_failure(self):
        with self._lock:
            self._failures += 1
            self._last_failure_time = time.time()
            if self._failures >= self.failure_threshold:
                self._state = "OPEN"

def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 10.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """Decorator for circuit breaker."""
    return CircuitBreaker(
        failure_threshold=failure_threshold,
        recovery_timeout=recovery_timeout,
        exceptions=exceptions
    )

def retry(
    max_retries: int = 3,
    initial_delay: float = 0.1,
    max_delay: float = 2.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,)
):
    """Decorator for retrying functions."""
    return RetryWithBackoff(
        max_retries=max_retries,
        initial_delay=initial_delay,
        max_delay=max_delay,
        exceptions=exceptions
    )
