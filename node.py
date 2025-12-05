import argparse
import os
import sys
from concurrent import futures
from typing import Optional

import grpc

# Ensure local imports resolve when executed from scripts directory.
sys.path.append(os.path.dirname(__file__))

import overlay_pb2
import overlay_pb2_grpc
from overlay_core import OverlayConfig, QueryOrchestrator


class OverlayService(overlay_pb2_grpc.OverlayNodeServicer):
    """Thin gRPC service that delegates behavior to QueryOrchestrator."""

    def __init__(self, orchestrator: QueryOrchestrator):
        self._orchestrator = orchestrator

    def Query(self, request, context):  # pylint: disable=invalid-name
        return self._orchestrator.execute_query(request)

    def GetChunk(self, request, context):  # pylint: disable=invalid-name
        return self._orchestrator.get_chunk(request.uid, request.chunk_index)

    def GetMetrics(self, request, context):  # pylint: disable=invalid-name
        return self._orchestrator.build_metrics_response()

    def Shutdown(self, request, context):  # pylint: disable=invalid-name
        return overlay_pb2.ShutdownResponse(status="noop")


def serve(
    config_path: str,
    process_id: str,
    dataset_root: str,
    chunk_size: Optional[int] = None,
    ttl: int = 300,
    fairness_strategy: Optional[str] = None,
):
    config = OverlayConfig(config_path)
    process = config.get(process_id)
    
    # Get global strategies from config, allow CLI overrides
    strategies = config.get_strategies()
    final_fairness = fairness_strategy if fairness_strategy is not None else strategies.fairness_strategy
    final_chunk_size = chunk_size if chunk_size is not None else strategies.chunk_size
    
    orchestrator = QueryOrchestrator(
        config=config,
        process=process,
        dataset_root=dataset_root,
        chunk_size=final_chunk_size,
        result_ttl=ttl,
        fairness_strategy=final_fairness,
    )

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    overlay_pb2_grpc.add_OverlayNodeServicer_to_server(OverlayService(orchestrator), server)
    server.add_insecure_port(f"0.0.0.0:{process.port}")

    server.start()
    print(
        f"[Overlay] {process.id} ({process.role}/{process.team}) "
        f"listening on {process.host}:{process.port}, dataset={dataset_root}",
        flush=True
    )
    server.wait_for_termination()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Start an overlay process.")
    parser.add_argument("config", help="Path to JSON overlay configuration.")
    parser.add_argument("process_id", help="Process identifier (e.g., A, B, C).")
    parser.add_argument(
        "--dataset-root",
        default="datasets/2020-fire/data",
        help="Root folder for dataset partitions.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=None,
        help="Chunk size for responses (overrides config default: 200).",
    )
    parser.add_argument("--result-ttl", type=int, default=300, help="Seconds to retain query results.")
    parser.add_argument(
        "--fairness-strategy",
        choices=["strict", "weighted", "hybrid"],
        default=None,
        help="Fairness strategy (overrides config).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    serve(
        args.config,
        args.process_id,
        args.dataset_root,
        args.chunk_size,
        args.result_ttl,
        args.fairness_strategy,
    )