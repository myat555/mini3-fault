import json
import threading
import time
import uuid
from collections import deque
from typing import Dict, List, Optional

import overlay_pb2

from .config import OverlayConfig, ProcessSpec
from .data_store import DataStore
from .metrics import MetricsTracker
from .proxies import NeighborRegistry
from .request_controller import RequestAdmissionController
from .result_cache import ChunkedResult, ResultCache
from .strategies import (
    FairnessStrategy,
    StrictPerTeamFairness,
    WeightedFairness,
    HybridFairness,
)
from .state_tracker import SimpleStateTracker, NodeState
from .hooks import HookManager, HookEvents
from .health_checker import HealthChecker
from .dynamic_router import DynamicRouter


class QueryOrchestrator:
    """
    Orchestrates query execution across the overlay network.
    Coordinates caching, fairness controls, and neighbor communication.
    """

    def __init__(
        self,
        config: OverlayConfig,
        process: ProcessSpec,
        dataset_root: str,
        chunk_size: int = 200,
        result_ttl: int = 300,
        default_limit: int = 2000,
        fairness_strategy: Optional[str] = "strict",
    ):
        self._config = config
        self._process = process
        team_members = self._compute_team_members(process.team)
        bounds = None
        if process.date_bounds and len(process.date_bounds) == 2:
            bounds = (process.date_bounds[0], process.date_bounds[1])
        self._data_store = DataStore(
            process.id,
            process.team,
            dataset_root=dataset_root,
            date_bounds=bounds,
            team_members=team_members,
        )
        
        # Initialize fairness strategy
        fairness = self._create_fairness_strategy(fairness_strategy)
        
        self._cache = ResultCache(ttl_seconds=result_ttl)
        self._admission = RequestAdmissionController(fairness_strategy=fairness)
        self._metrics = MetricsTracker()
        self._neighbor_registry = NeighborRegistry(config, process.id)
        self._chunk_size = chunk_size  # Fixed chunk size
        self._default_limit = default_limit
        self._log_buffer = deque(maxlen=50)  # Store last 50 log lines
        self._log_lock = threading.Lock()
        
        # Initialize state tracker and hooks
        self._state_tracker = SimpleStateTracker()
        self._hooks = HookManager(max_workers=8, name=f"HookManager-{process.id}")
        
        # Initialize health checker and dynamic router
        self._health_checker = HealthChecker(
            config=config,
            self_id=process.id,
            hook_manager=self._hooks,
            check_interval=5.0,
        )
        self._dynamic_router = DynamicRouter(self._health_checker)
        
        # Start health checking
        self._health_checker.start()
        
        # Register default hooks for state changes and neighbor events
        self._hooks.register_hook(
            HookEvents.STATE_CHANGED,
            self._on_state_changed,
            priority=10
        )
        self._hooks.register_hook(
            HookEvents.NEIGHBOR_FAILED,
            self._on_neighbor_failed,
            priority=10
        )
        self._hooks.register_hook(
            HookEvents.NEIGHBOR_RECOVERED,
            self._on_neighbor_recovered,
            priority=10
        )
        
        # Log initialization
        current_state = self._state_tracker.get_current_state()
        registered_events = self._hooks.get_registered_events()
        print(f"[Orchestrator] {process.id} initialized: StateTracker={current_state.value}, "
              f"HookManager={len(registered_events)} events registered, HealthChecker=active", flush=True)

    def _compute_team_members(self, team: str) -> List[ProcessSpec]:
        """Collect process specs that belong to the same team as this node."""
        team_lower = (team or "").lower()
        return [
            spec
            for spec in self._config.all_processes().values()
            if spec.team.lower() == team_lower
        ]

    def _compute_leader_allocations(self, neighbor_count: int, total_limit: int) -> List[int]:
        if neighbor_count <= 0:
            return []
        total_limit = max(1, int(total_limit))
        base = max(1, total_limit // neighbor_count)
        allocations = [base for _ in range(neighbor_count)]
        remainder = total_limit - base * neighbor_count
        idx = 0
        while remainder > 0:
            allocations[idx % neighbor_count] += 1
            remainder -= 1
            idx += 1
        return allocations

    def execute_query(self, request: overlay_pb2.QueryRequest) -> overlay_pb2.QueryResponse:
        hops = list(request.hops)
        if self._process.id in hops:
            log_msg = f"[Orchestrator] {self._process.id} detected loop, hops={hops}"
            print(log_msg, flush=True)
            self._add_log(log_msg)
            return overlay_pb2.QueryResponse(
                uid="",
                total_chunks=0,
                total_records=0,
                hops=hops,
                status="loop_detected",
            )
        hops.append(self._process.id)
        
        entry_msg = f"[Orchestrator] {self._process.id} received query, hops={request.hops}, client={request.client_id}"
        print(entry_msg, flush=True)
        self._add_log(entry_msg)

        try:
            filters = self._parse_filters(request.query_params)
        except ValueError as exc:
            error_msg = f"[Orchestrator] {self._process.id} invalid query params: {exc}"
            print(error_msg, flush=True)
            self._add_log(error_msg)
            return overlay_pb2.QueryResponse(
                uid="",
                total_chunks=0,
                total_records=0,
                hops=hops,
                status=f"invalid_query:{exc}",
            )

        uid = str(uuid.uuid4())
        target_team = filters.get("team") or self._process.team
        
        query_info = f"[Orchestrator] {self._process.id} query {uid[:8]}: filters={filters.get('parameter', 'any')}, limit={filters.get('limit', 'default')}, target_team={target_team}"
        print(query_info, flush=True)
        self._add_log(query_info)

        if not self._admission.admit(uid, target_team):
            reject_msg = f"[Orchestrator] {self._process.id} query {uid[:8]} REJECTED (admission control)"
            print(reject_msg, flush=True)
            self._add_log(reject_msg)
            return overlay_pb2.QueryResponse(
                uid="",
                total_chunks=0,
                total_records=0,
                hops=hops,
                status="rejected",
            )

        # Trigger query_started hook asynchronously
        self._hooks.trigger_hook(
            HookEvents.QUERY_STARTED,
            uid=uid,
            client_id=request.client_id,
            filters=filters,
            hops=hops,
        )

        start = time.time()
        try:
            records = self._collect_records(filters, hops, request.client_id, request.query_type)
            
            chunked = ChunkedResult(
                uid=uid,
                records=records,
                chunk_size=self._chunk_size,
                ttl_seconds=self._cache.ttl,
                metadata={
                    "process": self._process.id,
                    "team": self._process.team,
                    "filters": filters,
                    "fairness_strategy": self._admission._fairness.__class__.__name__,
                },
            )
            self._cache.store(chunked)
            duration_ms = (time.time() - start) * 1000
            self._metrics.record_completion(duration_ms)
            
            # Update state tracker
            stats = self._metrics.snapshot()
            admission = self._admission.snapshot()
            state_change = self._state_tracker.update_state(
                avg_latency_ms=stats["avg_ms"],
                active_requests=admission["active"],
                queue_size=len(self._cache),
            )
            
            # Get current state after update
            current_state = self._state_tracker.get_current_state()
            
            # Log state tracker activity (every query)
            print(f"[StateTracker] {self._process.id} state: {current_state.value} "
                  f"(latency={stats['avg_ms']:.1f}ms, active={admission['active']}, queue={len(self._cache)})", 
                  flush=True)
            
            # Trigger state_changed hook if state changed
            if state_change:
                old_state, new_state, reason = state_change
                self._hooks.trigger_hook(
                    HookEvents.STATE_CHANGED,
                    old_state=old_state,
                    new_state=new_state,
                    reason=reason,
                    process_id=self._process.id,
                )
            
            # Trigger query_completed hook asynchronously
            self._hooks.trigger_hook(
                HookEvents.QUERY_COMPLETED,
                uid=uid,
                duration_ms=duration_ms,
                records_count=len(records),
                status="success",
            )
            
            filter_summary = f"param={filters.get('parameter', 'any')}"
            if 'min_value' in filters or 'max_value' in filters:
                filter_summary += f", value=[{filters.get('min_value', '')}, {filters.get('max_value', '')}]"
            
            if self._process.role == "leader":
                log_msg = f"[Orchestrator] {self._process.id} coordinated query {uid[:8]}: aggregated {len(records)} records from team leaders, {duration_ms:.1f}ms, filters={{{filter_summary}}}"
            else:
                log_msg = f"[Orchestrator] {self._process.id} query {uid[:8]}: {len(records)} records, {duration_ms:.1f}ms, filters={{{filter_summary}}}"
            print(log_msg, flush=True)
            self._add_log(log_msg)

            return overlay_pb2.QueryResponse(
                uid=uid,
                total_chunks=chunked.total_chunks,
                total_records=chunked.total_records,
                hops=hops,
                status="ready",
            )
        finally:
            self._admission.release(uid)

    def get_chunk(self, uid: str, chunk_index: int) -> overlay_pb2.ChunkResponse:
        result = self._cache.get(uid)
        if not result:
            return overlay_pb2.ChunkResponse(
                uid=uid,
                chunk_index=chunk_index,
                total_chunks=0,
                data="[]",
                is_last=True,
                status="not_found",
            )

        chunk = result.get_chunk(chunk_index)
        if not chunk:
            return overlay_pb2.ChunkResponse(
                uid=uid,
                chunk_index=chunk_index,
                total_chunks=result.total_chunks,
                data="[]",
                is_last=True,
                status="out_of_range",
            )

        if chunk["is_last"]:
            self._cache.delete(uid)

        return overlay_pb2.ChunkResponse(
            uid=uid,
            chunk_index=chunk["chunk_index"],
            total_chunks=chunk["total_chunks"],
            data=json.dumps(chunk["data"]),
            is_last=chunk["is_last"],
            status="success",
        )

    def _add_log(self, message: str) -> None:
        """Add a log message to the buffer."""
        with self._log_lock:
            self._log_buffer.append(message)
    
    def _get_recent_logs(self, max_lines: int = 10) -> List[str]:
        """Get recent log lines from buffer."""
        with self._log_lock:
            return list(self._log_buffer)[-max_lines:]
    
    def build_metrics_response(self) -> overlay_pb2.MetricsResponse:
        stats = self._metrics.snapshot()
        admission = self._admission.snapshot()
        recent_logs = self._get_recent_logs(max_lines=10)
        
        # Get state tracker snapshot
        state_snapshot = self._state_tracker.snapshot()
        current_state = self._state_tracker.get_current_state()
        
        # Add state info to logs
        state_log = f"State: {current_state.value}"
        if state_snapshot.get("total_transitions", 0) > 0:
            state_log += f", Transitions: {state_snapshot['total_transitions']}"
        recent_logs.append(state_log)
        
        # Add health checker info
        health_snapshot = self._health_checker.snapshot()
        if health_snapshot.get("failed_neighbors"):
            recent_logs.append(f"Failed neighbors: {', '.join(health_snapshot['failed_neighbors'])}")
        
        return overlay_pb2.MetricsResponse(
            process_id=self._process.id,
            role=self._process.role,
            team=self._process.team,
            active_requests=admission["active"],
            max_capacity=self._admission.max_active,
            is_healthy=current_state == NodeState.HEALTHY,
            queue_size=len(self._cache),
            avg_processing_time_ms=float(stats["avg_ms"]),
            data_files_loaded=self._data_store.files_loaded if self._data_store else 0,
            fairness_strategy=self._admission._fairness.__class__.__name__,
            recent_logs=recent_logs,
        )

    def _parse_filters(self, raw_params: str) -> Dict[str, object]:
        filters = json.loads(raw_params) if raw_params else {}
        if not isinstance(filters, dict):
            raise ValueError("query_params must decode into a JSON object.")
        limit = filters.get("limit") or self._default_limit
        filters["limit"] = max(1, min(int(limit), self._default_limit))
        return filters

    def _collect_records(
        self,
        filters: Dict[str, object],
        hops: List[str],
        client_id: Optional[str],
        query_type: Optional[str],
    ) -> List[Dict[str, object]]:
        collect_msg = f"[Orchestrator] {self._process.id} _collect_records called, role={self._process.role}, limit={filters.get('limit', self._default_limit)}"
        print(collect_msg, flush=True)
        self._add_log(collect_msg)
        
        aggregated: List[Dict[str, object]] = []
        total_limit = filters.get("limit", self._default_limit)
        remaining = total_limit

        # Leaders and team leaders forward first (coordination role)
        # Workers query locally first, then forward if needed
        if self._process.role in ("leader", "team_leader"):
            # Forward to subordinates first
            neighbors = self._select_forward_targets()
            debug_msg = f"[Orchestrator] {self._process.id} _select_forward_targets returned {len(neighbors)} neighbors: {[n.id for n in neighbors]}"
            print(debug_msg, flush=True)
            self._add_log(debug_msg)
            
            if neighbors:
                allocations = self._compute_leader_allocations(len(neighbors), total_limit)
                team_hint = (
                    None if self._process.role == "leader" else self._process.team
                )
                for neighbor, allocation in zip(neighbors, allocations):
                    log_msg = f"[Orchestrator] {self._process.id} forwarding to {neighbor.id} ({neighbor.role}/{neighbor.team}), allocation={allocation}, remaining={remaining}"
                    print(log_msg, flush=True)
                    self._add_log(log_msg)
                    try:
                        remote_rows = self._request_neighbor_records(
                            neighbor,
                            filters,
                            hops,
                            client_id,
                            allocation,
                            team_hint=team_hint or neighbor.team,
                        )
                        aggregated.extend(remote_rows)
                        remaining -= len(remote_rows)
                        result_msg = f"[Orchestrator] {self._process.id} received {len(remote_rows)} records from {neighbor.id}, remaining={remaining}"
                        print(result_msg, flush=True)
                        self._add_log(result_msg)
                    except Exception as exc:
                        error_msg = f"[Orchestrator] {self._process.id} failed forwarding to {neighbor.id}: {exc}"
                        print(error_msg, flush=True)
                        self._add_log(error_msg)
            else:
                no_neighbors_msg = f"[Orchestrator] {self._process.id} no neighbors to forward to, will query locally"
                print(no_neighbors_msg, flush=True)
                self._add_log(no_neighbors_msg)
            
            # After forwarding, query local data if still needed
            if remaining > 0 and self._data_store is not None:
                local_rows = self._data_store.query(filters, limit=remaining)
                if local_rows:
                    log_msg = f"[Orchestrator] {self._process.id} local query: {len(local_rows)} records from {self._data_store.records_loaded} total"
                    print(log_msg, flush=True)
                    self._add_log(log_msg)
                aggregated.extend(local_rows)
                remaining -= len(local_rows)
        else:
            # Workers: query local data first, then forward if needed
            if self._data_store is not None:
                local_rows = self._data_store.query(filters, limit=remaining)
                if local_rows:
                    log_msg = f"[Orchestrator] {self._process.id} local query: {len(local_rows)} records from {self._data_store.records_loaded} total"
                    print(log_msg, flush=True)
                    self._add_log(log_msg)
                aggregated.extend(local_rows)
                remaining -= len(local_rows)
            
            # Forward to neighbors if still needed
            if remaining > 0:
                neighbors = self._select_forward_targets()
                for neighbor in neighbors:
                    if remaining <= 0:
                        break
                    try:
                        rows = self._request_neighbor_records(
                            neighbor,
                            filters,
                            hops,
                            client_id,
                            remaining,
                            team_hint=neighbor.team,
                        )
                        aggregated.extend(rows)
                        remaining -= len(rows)
                    except Exception as exc:
                        print(f"[Orchestrator] Failed forwarding to {neighbor.id}: {exc}", flush=True)

        return aggregated[: total_limit]

    def _select_forward_targets(self) -> List[ProcessSpec]:
        neighbors = self._config.neighbors_of(self._process.id)
        if not neighbors:
            return []

        if self._process.role == "leader":
            # Leader forwards to team leaders
            neighbors = [n for n in neighbors if n.role == "team_leader"]
        elif self._process.role == "team_leader":
            # Team leaders forward to their own team workers first
            own_team_workers = [n for n in neighbors if n.team == self._process.team and n.role == "worker"]
            if own_team_workers:
                neighbors = own_team_workers
            else:
                # Fallback to cross-team workers if no own-team workers
                neighbors = [n for n in neighbors if n.role == "worker"]
        else:
            # Workers don't forward
            neighbors = []

        # Filter to healthy neighbors only using dynamic router
        neighbors = self._dynamic_router.filter_healthy(neighbors)
        
        return neighbors

    def _request_neighbor_records(
        self,
        neighbor: ProcessSpec,
        filters: Dict[str, object],
        hops: List[str],
        client_id: Optional[str],
        limit: int,
        team_hint: Optional[str] = None,
    ) -> List[Dict[str, object]]:
        client = self._neighbor_registry.for_neighbor(neighbor.id)

        forward_filters = dict(filters)
        forward_filters["limit"] = max(1, int(limit))
        if team_hint:
            forward_filters["team"] = team_hint
        forward_request = overlay_pb2.QueryRequest(
            query_type="filter",
            query_params=json.dumps(forward_filters),
            hops=hops,
            client_id=client_id or self._process.id,
        )

        log_msg = f"[Orchestrator] {self._process.id} forwarding to {neighbor.id} ({neighbor.role}/{neighbor.team}), remaining={forward_filters['limit']}"
        print(log_msg, flush=True)
        self._add_log(log_msg)

        try:
            response = client.query(forward_request)
        except Exception as exc:
            log_msg = f"[Orchestrator] Failed forwarding to {neighbor.id} ({neighbor.address}): {exc}"
            print(log_msg, flush=True)
            self._add_log(log_msg)
            return []

        if response.status != "ready" or not response.uid:
            return []

        return self._drain_remote_chunks(client, response.uid, response.total_chunks, forward_filters["limit"])

    @staticmethod
    def _safe_json_loads(payload: str) -> List[Dict[str, object]]:
        try:
            data = json.loads(payload) if payload else []
            if isinstance(data, list):
                return data
        except json.JSONDecodeError:
            pass
        return []

    def _drain_remote_chunks(
        self,
        client,
        remote_uid: str,
        total_chunks: int,
        remaining: int,
    ) -> List[Dict[str, object]]:
        collected: List[Dict[str, object]] = []
        for idx in range(total_chunks):
            if remaining <= 0:
                break
            chunk_resp = client.get_chunk(remote_uid, idx)
            if chunk_resp.status != "success":
                break
            rows = self._safe_json_loads(chunk_resp.data)
            for row in rows:
                collected.append(row)
                remaining -= 1
                if remaining <= 0:
                    break
            if chunk_resp.is_last:
                break
        return collected

    def _create_fairness_strategy(self, strategy_name: str) -> FairnessStrategy:
        """Create fairness strategy instance."""
        strategy_name = (strategy_name or "strict").lower()
        if strategy_name == "weighted":
            return WeightedFairness()
        elif strategy_name == "hybrid":
            return HybridFairness()
        else:  # strict (default)
            return StrictPerTeamFairness()
    
    def _on_state_changed(self, old_state: NodeState, new_state: NodeState, reason: str, process_id: str):
        """Default hook handler for state changes."""
        log_msg = (
            f"[StateTracker] {process_id} state transition: "
            f"{old_state.value} -> {new_state.value} ({reason})"
        )
        print(log_msg, flush=True)
        self._add_log(log_msg)
    
    def _on_neighbor_failed(self, neighbor_id: str, process_id: str, **kwargs):
        """Handle neighbor failure."""
        log_msg = f"[Orchestrator] {process_id} handling failure of {neighbor_id}"
        print(log_msg, flush=True)
        self._add_log(log_msg)
    
    def _on_neighbor_recovered(self, neighbor_id: str, process_id: str, **kwargs):
        """Handle neighbor recovery."""
        log_msg = f"[Orchestrator] {process_id} handling recovery of {neighbor_id}"
        print(log_msg, flush=True)
        self._add_log(log_msg)
    
    def shutdown(self):
        """Shutdown orchestrator and cleanup resources."""
        self._health_checker.stop()
        self._hooks.shutdown(wait=True, timeout=5.0)