"""Shared infrastructure for overlay nodes (query orchestration, neighbor connections, data access)."""

from .config import ProcessSpec, OverlayConfig, StrategyConfig
from .data_store import DataStore
from .result_cache import ResultCache, ChunkedResult
from .request_controller import RequestAdmissionController
from .metrics import MetricsTracker
from .proxies import NeighborRegistry, RemoteNodeClient
from .facade import QueryOrchestrator
from .strategies import (
    FairnessStrategy,
    StrictPerTeamFairness,
    WeightedFairness,
    HybridFairness,
)

__all__ = [
    "ProcessSpec",
    "OverlayConfig",
    "StrategyConfig",
    "DataStore",
    "ResultCache",
    "ChunkedResult",
    "RequestAdmissionController",
    "MetricsTracker",
    "NeighborRegistry",
    "RemoteNodeClient",
    "QueryOrchestrator",
    "FairnessStrategy",
    "StrictPerTeamFairness",
    "WeightedFairness",
    "HybridFairness",
]

