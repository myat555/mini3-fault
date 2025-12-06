"""Health checking and failure tracking for fault tolerance."""

import threading
import time
from typing import Dict, List, Optional, Set
import grpc
import overlay_pb2
import overlay_pb2_grpc
from .config import OverlayConfig, ProcessSpec
from .hooks import HookManager, HookEvents


class HealthChecker:
    """Continuously checks neighbor health and tracks failures."""
    
    def __init__(
        self,
        config: OverlayConfig,
        self_id: str,
        hook_manager: HookManager,
        check_interval: float = 5.0,
        failure_threshold: int = 2,  # Consecutive failures before marking as down
    ):
        self._config = config
        self._self_id = self_id
        self._hooks = hook_manager
        self._interval = check_interval
        self._failure_threshold = failure_threshold
        
        self._lock = threading.Lock()
        self._healthy_neighbors: Set[str] = set()
        self._failed_neighbors: Set[str] = set()
        self._failure_counts: Dict[str, int] = {}  # neighbor_id -> consecutive failures
        self._last_check_time: Dict[str, float] = {}
        self._recovery_time: Dict[str, float] = {}  # When neighbor recovered
        
        self._active = False
        self._thread: Optional[threading.Thread] = None
        
        # Initialize with all neighbors as healthy
        neighbors = config.neighbors_of(self_id)
        self._healthy_neighbors = {n.id for n in neighbors}
    
    def start(self):
        """Start health checking thread."""
        if self._active:
            return
        self._active = True
        self._thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self._thread.start()
        print(f"[HealthChecker] {self._self_id} started health checking", flush=True)
    
    def stop(self):
        """Stop health checking."""
        self._active = False
        if self._thread:
            self._thread.join(timeout=2)
    
    def _health_check_loop(self):
        """Main health checking loop."""
        while self._active:
            try:
                self._check_all_neighbors()
            except Exception as e:
                print(f"[HealthChecker] Error in health check loop: {e}", flush=True)
            
            time.sleep(self._interval)
    
    def _check_all_neighbors(self):
        """Check health of all neighbors."""
        neighbors = self._config.neighbors_of(self._self_id)
        
        for neighbor in neighbors:
            is_healthy = self._check_neighbor(neighbor)
            self._update_neighbor_status(neighbor.id, is_healthy)
    
    def _check_neighbor(self, neighbor: ProcessSpec) -> bool:
        """Check if a single neighbor is healthy. Returns True if healthy."""
        try:
            address = neighbor.address
            with grpc.insecure_channel(
                address,
                options=[
                    ("grpc.keepalive_timeout_ms", 1000),
                    ("grpc.keepalive_time_ms", 1000),
                ]
            ) as channel:
                stub = overlay_pb2_grpc.OverlayNodeStub(channel)
                stub.GetMetrics(overlay_pb2.MetricsRequest(), timeout=1)
                return True
        except grpc.RpcError as e:
            # gRPC errors (timeout, unavailable, etc.)
            return False
        except Exception as e:
            # Other errors
            return False
    
    def _update_neighbor_status(self, neighbor_id: str, is_healthy: bool):
        """Update neighbor status and trigger hooks on state changes."""
        with self._lock:
            was_healthy = neighbor_id in self._healthy_neighbors
            was_failed = neighbor_id in self._failed_neighbors
            
            if is_healthy:
                # Neighbor is healthy
                if neighbor_id in self._failed_neighbors:
                    # Recovery detected
                    self._failed_neighbors.remove(neighbor_id)
                    self._healthy_neighbors.add(neighbor_id)
                    self._failure_counts[neighbor_id] = 0
                    self._recovery_time[neighbor_id] = time.time()
                    
                    print(f"[HealthChecker] {self._self_id} detected recovery: {neighbor_id}", flush=True)
                    self._hooks.trigger_hook(
                        HookEvents.NEIGHBOR_RECOVERED,
                        neighbor_id=neighbor_id,
                        process_id=self._self_id,
                    )
                else:
                    # Still healthy
                    self._healthy_neighbors.add(neighbor_id)
                    self._failure_counts[neighbor_id] = 0
            else:
                # Neighbor is unhealthy
                self._failure_counts[neighbor_id] = self._failure_counts.get(neighbor_id, 0) + 1
                self._last_check_time[neighbor_id] = time.time()
                
                # Mark as failed if threshold reached
                if self._failure_counts[neighbor_id] >= self._failure_threshold:
                    if neighbor_id in self._healthy_neighbors:
                        # New failure detected
                        self._healthy_neighbors.remove(neighbor_id)
                        self._failed_neighbors.add(neighbor_id)
                        
                        print(f"[HealthChecker] {self._self_id} detected failure: {neighbor_id} "
                              f"({self._failure_counts[neighbor_id]} consecutive failures)", flush=True)
                        self._hooks.trigger_hook(
                            HookEvents.NEIGHBOR_FAILED,
                            neighbor_id=neighbor_id,
                            process_id=self._self_id,
                            failure_count=self._failure_counts[neighbor_id],
                        )
    
    def is_healthy(self, neighbor_id: str) -> bool:
        """Check if a neighbor is currently healthy."""
        with self._lock:
            return neighbor_id in self._healthy_neighbors
    
    def get_healthy_neighbors(self) -> List[ProcessSpec]:
        """Get list of currently healthy neighbors."""
        neighbors = self._config.neighbors_of(self._self_id)
        with self._lock:
            return [n for n in neighbors if n.id in self._healthy_neighbors]
    
    def get_failed_neighbors(self) -> Set[str]:
        """Get set of failed neighbor IDs."""
        with self._lock:
            return set(self._failed_neighbors)
    
    def snapshot(self) -> Dict:
        """Get snapshot of health checker state."""
        with self._lock:
            return {
                "healthy_neighbors": list(self._healthy_neighbors),
                "failed_neighbors": list(self._failed_neighbors),
                "failure_counts": dict(self._failure_counts),
                "recovery_times": dict(self._recovery_time),
            }

