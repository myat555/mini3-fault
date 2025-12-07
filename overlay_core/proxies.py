from typing import Dict
import threading

import grpc

import overlay_pb2
import overlay_pb2_grpc

from .config import OverlayConfig, ProcessSpec
from .resilience import retry, CircuitBreaker

class RemoteNodeClient:
    """Client for communicating with remote overlay nodes via gRPC."""

    def __init__(self, spec: ProcessSpec, channel: grpc.Channel):
        self.spec = spec
        self._channel = channel
        self._stub = overlay_pb2_grpc.OverlayNodeStub(self._channel)
        self._circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=10.0, exceptions=(grpc.RpcError,))

    @property
    def address(self) -> str:
        return self.spec.address

    @retry(max_retries=3, initial_delay=0.2, exceptions=(grpc.RpcError,))
    def query(self, request: overlay_pb2.QueryRequest) -> overlay_pb2.QueryResponse:
        if not self._circuit_breaker.allow_request():
            raise RuntimeError("CircuitBreaker is OPEN")
            
        try:
            # Re-uses the channel and stub
            response = self._stub.Query(request)
            self._circuit_breaker.record_success()
            return response
        except grpc.RpcError as e:
            self._circuit_breaker.record_failure()
            raise e

    @retry(max_retries=3, initial_delay=0.2, exceptions=(grpc.RpcError,))
    def get_chunk(self, uid: str, index: int) -> overlay_pb2.ChunkResponse:
        if not self._circuit_breaker.allow_request():
            raise RuntimeError("CircuitBreaker is OPEN")
            
        try:
            chunk_request = overlay_pb2.ChunkRequest(uid=uid, chunk_index=index)
            response = self._stub.GetChunk(chunk_request)
            self._circuit_breaker.record_success()
            return response
        except grpc.RpcError as e:
            self._circuit_breaker.record_failure()
            raise e


class NeighborRegistry:
    """Manages connections to neighbor nodes in the overlay network."""

    def __init__(self, config: OverlayConfig, self_id: str):
        self._config = config
        self._self_id = self_id
        self._clients: Dict[str, RemoteNodeClient] = {}
        self._channels: Dict[str, grpc.Channel] = {}
        self._lock = threading.Lock()

    def for_neighbor(self, neighbor_id: str) -> RemoteNodeClient:
        if neighbor_id == self._self_id:
            raise ValueError("Cannot create client for self.")

        # First, try to get the client without a lock for performance.
        client = self._clients.get(neighbor_id)
        if client is not None:
            return client

        # If not found, acquire lock and double-check to handle race conditions.
        with self._lock:
            client = self._clients.get(neighbor_id)
            if client is not None:
                return client

            spec = self._config.get(neighbor_id)
            # gRPC Keepalive options to ensure connection resilience
            keepalive_options = [
                # Send a ping every 10 seconds to keep the connection alive
                ('grpc.keepalive_time_ms', 10000),
                # Wait 5 seconds for a ping response before considering the connection down
                ('grpc.keepalive_timeout_ms', 5000),
                # Allow pings even if there are no ongoing calls
                ('grpc.keepalive_permit_without_calls', True),
                # Minimum time between pings
                ('grpc.http2.min_time_between_pings_ms', 10000),
                # Allow unlimited pings without data
                ('grpc.http2.max_pings_without_data', 0),
            ]
            channel = grpc.insecure_channel(spec.address, options=keepalive_options)
            self._channels[neighbor_id] = channel
            client = RemoteNodeClient(spec, channel)
            self._clients[neighbor_id] = client
            return client

    def close_all(self):
        """Closes all open gRPC channels."""
        with self._lock:
            for channel in self._channels.values():
                channel.close()
            self._channels.clear()
            self._clients.clear()
