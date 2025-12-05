"""Hook system for temporal decoupling and asynchronous callbacks."""

import threading
import time
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Tuple


class HookManager:
    """
    Manages hooks for temporal decoupling.
    Allows registering callbacks that execute asynchronously.
    """

    def __init__(self, max_workers: int = 8, name: str = "HookManager"):
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix=name
        )
        self._hooks: Dict[str, List[Tuple[int, Callable]]] = defaultdict(list)
        self._lock = threading.Lock()
        self._hook_stats: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"triggered": 0, "errors": 0}
        )
        self._active_futures: List[Future] = []
        self._max_active_futures = 100  # Limit concurrent futures

    def register_hook(self, event: str, callback: Callable, priority: int = 0):
        """
        Register a callback for an event.
        
        Args:
            event: Event name (e.g., "query_started", "query_completed")
            callback: Function to call when event is triggered
            priority: Higher priority callbacks execute first (0 = default)
        """
        with self._lock:
            # Store with priority for ordering
            self._hooks[event].append((priority, callback))
            # Sort by priority (higher first)
            self._hooks[event].sort(key=lambda x: x[0], reverse=True)

    def unregister_hook(self, event: str, callback: Callable):
        """Unregister a callback from an event."""
        with self._lock:
            if event in self._hooks:
                self._hooks[event] = [
                    (p, cb) for p, cb in self._hooks[event] if cb != callback
                ]
                if not self._hooks[event]:
                    del self._hooks[event]

    def trigger_hook(
        self, event: str, *args, **kwargs
    ) -> List[Future]:
        """
        Trigger all callbacks for an event asynchronously.
        
        Returns:
            List of Future objects for the triggered callbacks
        """
        with self._lock:
            callbacks = [(p, cb) for p, cb in self._hooks.get(event, [])]
            self._hook_stats[event]["triggered"] += len(callbacks)

        futures = []
        for priority, callback in callbacks:
            try:
                future = self._executor.submit(self._safe_call, callback, event, *args, **kwargs)
                futures.append(future)
                
                # Clean up old completed futures
                self._cleanup_futures()
                
            except Exception as e:
                print(f"[HookManager] Error submitting hook '{event}': {e}", flush=True)
                with self._lock:
                    self._hook_stats[event]["errors"] += 1

        return futures

    def _safe_call(self, callback: Callable, event: str, *args, **kwargs):
        """Safely call a callback, catching exceptions."""
        try:
            return callback(*args, **kwargs)
        except Exception as e:
            print(f"[HookManager] Hook '{event}' callback error: {e}", flush=True)
            with self._lock:
                self._hook_stats[event]["errors"] += 1
            raise

    def _cleanup_futures(self):
        """Remove completed futures to prevent memory buildup."""
        with self._lock:
            self._active_futures = [
                f for f in self._active_futures if not f.done()
            ]

    def wait_for_hooks(
        self, futures: List[Future], timeout: Optional[float] = None
    ) -> List[Any]:
        """
        Wait for hook futures to complete and return results.
        
        Args:
            futures: List of Future objects from trigger_hook
            timeout: Maximum time to wait (None = wait forever)
        
        Returns:
            List of results from callbacks (None for exceptions)
        """
        results = []
        for future in futures:
            try:
                result = future.result(timeout=timeout)
                results.append(result)
            except Exception as e:
                print(f"[HookManager] Future error: {e}", flush=True)
                results.append(None)
        return results

    def trigger_hook_sync(self, event: str, *args, **kwargs) -> List[Any]:
        """
        Trigger hooks synchronously (wait for completion).
        Useful for critical hooks that must complete before continuing.
        """
        futures = self.trigger_hook(event, *args, **kwargs)
        return self.wait_for_hooks(futures)

    def get_registered_events(self) -> List[str]:
        """Get list of events that have registered hooks."""
        with self._lock:
            return list(self._hooks.keys())

    def get_hook_count(self, event: str) -> int:
        """Get number of hooks registered for an event."""
        with self._lock:
            return len(self._hooks.get(event, []))

    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """Get statistics about hook usage."""
        with self._lock:
            return dict(self._hook_stats)

    def snapshot(self) -> Dict:
        """Get snapshot of hook manager state."""
        with self._lock:
            return {
                "registered_events": list(self._hooks.keys()),
                "event_counts": {
                    event: len(callbacks) for event, callbacks in self._hooks.items()
                },
                "stats": dict(self._hook_stats),
                "active_futures": len(self._active_futures),
            }

    def shutdown(self, wait: bool = True, timeout: Optional[float] = None):
        """Shutdown the hook manager and executor."""
        self._executor.shutdown(wait=wait, timeout=timeout)


# Predefined hook event names for consistency
class HookEvents:
    """Standard hook event names."""
    QUERY_STARTED = "query_started"
    QUERY_COMPLETED = "query_completed"
    QUERY_FAILED = "query_failed"
    STATE_CHANGED = "state_changed"
    NEIGHBOR_FAILED = "neighbor_failed"
    NEIGHBOR_RECOVERED = "neighbor_recovered"
    OVERLOAD_DETECTED = "overload_detected"
    DATA_REPLICATED = "data_replicated"
    MIGRATION_STARTED = "migration_started"
    MIGRATION_COMPLETED = "migration_completed"

