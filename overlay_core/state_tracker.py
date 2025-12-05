"""Simple state tracking for node health monitoring."""

import time
from collections import defaultdict
from enum import Enum
from typing import Dict, List, Optional, Tuple


class NodeState(Enum):
    """Node health states."""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    FAILING = "failing"


class SimpleStateTracker:
    """
    Tracks node state transitions based on metrics.
    Simple state machine that transitions based on thresholds.
    """

    def __init__(
        self,
        latency_warning_threshold: float = 1000.0,  # 1 second
        latency_critical_threshold: float = 5000.0,  # 5 seconds
        active_warning_threshold: int = 70,
        active_critical_threshold: int = 90,
        queue_warning_threshold: int = 30,
        queue_critical_threshold: int = 50,
    ):
        self._latency_warning = latency_warning_threshold
        self._latency_critical = latency_critical_threshold
        self._active_warning = active_warning_threshold
        self._active_critical = active_critical_threshold
        self._queue_warning = queue_warning_threshold
        self._queue_critical = queue_critical_threshold
        
        self._current_state = NodeState.HEALTHY
        self._state_history: List[Tuple[float, NodeState, NodeState, str]] = []
        self._transition_count: Dict[Tuple[NodeState, NodeState], int] = defaultdict(int)
        self._time_in_state: Dict[NodeState, float] = defaultdict(float)
        self._state_start_time = time.time()
        self._last_update_time = time.time()

    def update_state(
        self,
        avg_latency_ms: float,
        active_requests: int,
        queue_size: int = 0,
    ) -> Optional[Tuple[NodeState, NodeState, str]]:
        """
        Update state based on current metrics.
        
        Returns:
            (old_state, new_state, reason) if state changed, None otherwise
        """
        old_state = self._current_state
        reason = ""
        
        # Determine new state based on thresholds
        # Check for FAILING state first (most severe)
        if (avg_latency_ms > self._latency_critical and active_requests > self._active_critical) or \
           (avg_latency_ms > self._latency_critical * 2) or \
           (queue_size > self._queue_critical * 2):
            new_state = NodeState.FAILING
            reason = f"latency={avg_latency_ms:.1f}ms,active={active_requests},queue={queue_size}"
        
        # Check for CRITICAL state
        elif avg_latency_ms > self._latency_critical or \
             active_requests > self._active_critical or \
             queue_size > self._queue_critical:
            new_state = NodeState.CRITICAL
            reason = f"latency={avg_latency_ms:.1f}ms,active={active_requests},queue={queue_size}"
        
        # Check for WARNING state
        elif avg_latency_ms > self._latency_warning or \
             active_requests > self._active_warning or \
             queue_size > self._queue_warning:
            new_state = NodeState.WARNING
            reason = f"latency={avg_latency_ms:.1f}ms,active={active_requests},queue={queue_size}"
        
        # Otherwise HEALTHY
        else:
            new_state = NodeState.HEALTHY
            reason = "all_metrics_normal"
        
        # Update state if changed
        if old_state != new_state:
            now = time.time()
            time_in_old_state = now - self._state_start_time
            self._time_in_state[old_state] += time_in_old_state
            
            # Record transition
            transition_key = (old_state, new_state)
            self._transition_count[transition_key] += 1
            self._state_history.append((now, old_state, new_state, reason))
            
            # Update current state
            self._current_state = new_state
            self._state_start_time = now
            self._last_update_time = now
            
            return (old_state, new_state, reason)
        
        self._last_update_time = time.time()
        return None

    def get_current_state(self) -> NodeState:
        """Get current state."""
        return self._current_state

    def get_state_history(self, limit: int = 10) -> List[Tuple[float, NodeState, NodeState, str]]:
        """Get recent state transitions."""
        return self._state_history[-limit:] if self._state_history else []

    def get_most_common_transition(self) -> Optional[Tuple[NodeState, NodeState, int]]:
        """
        Get the most common state transition.
        Returns (from_state, to_state, count) or None if no transitions.
        """
        if not self._transition_count:
            return None
        
        most_common = max(self._transition_count.items(), key=lambda x: x[1])
        from_state, to_state = most_common[0]
        count = most_common[1]
        return (from_state, to_state, count)

    def get_transition_probability(
        self, from_state: NodeState, to_state: NodeState
    ) -> float:
        """
        Get probability of transitioning from one state to another.
        Based on historical transitions.
        """
        if not self._transition_count:
            return 0.0
        
        # Count all transitions from from_state
        total_from = sum(
            count for (f, _), count in self._transition_count.items() if f == from_state
        )
        
        if total_from == 0:
            return 0.0
        
        transition_count = self._transition_count.get((from_state, to_state), 0)
        return transition_count / total_from

    def get_time_in_state(self, state: NodeState) -> float:
        """Get total time spent in a state (seconds)."""
        current_time = time.time()
        if self._current_state == state:
            # Add current time in state
            return self._time_in_state[state] + (current_time - self._state_start_time)
        return self._time_in_state[state]

    def snapshot(self) -> Dict:
        """Get current snapshot of state tracker."""
        current_time = time.time()
        time_in_current = current_time - self._state_start_time if self._current_state else 0.0
        
        return {
            "current_state": self._current_state.value,
            "time_in_current_state": time_in_current,
            "total_transitions": len(self._state_history),
            "most_common_transition": (
                self.get_most_common_transition() or (None, None, 0)
            ),
            "time_in_states": {
                state.value: self.get_time_in_state(state)
                for state in NodeState
            },
            "transition_counts": {
                f"{f.value}->{t.value}": count
                for (f, t), count in self._transition_count.items()
            },
        }

