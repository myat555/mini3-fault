from typing import Dict

import grpc

import overlay_pb2
import overlay_pb2_grpc

from .config import OverlayConfig, ProcessSpec


class RemoteNodeClient:
    """Client for communicating with remote overlay nodes via gRPC."""

    def __init__(self, spec: ProcessSpec):
        self.spec = spec

    @property
    def address(self) -> str:
        return self.spec.address

    def query(self, request: overlay_pb2.QueryRequest) -> overlay_pb2.QueryResponse:
        with grpc.insecure_channel(self.address) as channel:
            stub = overlay_pb2_grpc.OverlayNodeStub(channel)
            return stub.Query(request)

    def get_chunk(self, uid: str, index: int) -> overlay_pb2.ChunkResponse:
        with grpc.insecure_channel(self.address) as channel:
            stub = overlay_pb2_grpc.OverlayNodeStub(channel)
            chunk_request = overlay_pb2.ChunkRequest(uid=uid, chunk_index=index)
            return stub.GetChunk(chunk_request)


class NeighborRegistry:
    """Manages connections to neighbor nodes in the overlay network."""

    def __init__(self, config: OverlayConfig, self_id: str):
        self._config = config
        self._self_id = self_id
        self._clients: Dict[str, RemoteNodeClient] = {}

    def for_neighbor(self, neighbor_id: str) -> RemoteNodeClient:
        if neighbor_id == self._self_id:
            raise ValueError("Cannot create client for self.")
        if neighbor_id not in self._clients:
            spec = self._config.get(neighbor_id)
            self._clients[neighbor_id] = RemoteNodeClient(spec)
        return self._clients[neighbor_id]

