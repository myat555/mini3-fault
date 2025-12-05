import json
import os
import sys
import time
from typing import Dict, Iterable

import grpc

sys.path.append(os.path.dirname(__file__))

import overlay_pb2
import overlay_pb2_grpc


def _open_stub(host: str, port: int):
    address = f"{host}:{port}"
    channel = grpc.insecure_channel(address)
    return address, channel, overlay_pb2_grpc.OverlayNodeStub(channel)


def send_query(host: str, port: int, query_params: Dict[str, object]) -> None:
    address, channel, stub = _open_stub(host, port)
    try:
        request = overlay_pb2.QueryRequest(
            query_type="filter",
            query_params=json.dumps(query_params),
            hops=[],
            client_id="cli",
        )
        start = time.time()
        response = stub.Query(request)
        latency_ms = (time.time() - start) * 1000
        print(f"Query to {address} completed in {latency_ms:.2f} ms")
        print(f"Status: {response.status}")
        print(f"Hops: {list(response.hops)}")

        if response.status != "ready" or not response.uid:
            return

        print(f"UID: {response.uid} | total_chunks={response.total_chunks}")
        for chunk in stream_chunks(stub, response.uid):
            print_chunk_summary(chunk)
    except Exception as exc:
        print(f"Query error: {exc}")
    finally:
        channel.close()


def stream_chunks(stub: overlay_pb2_grpc.OverlayNodeStub, uid: str) -> Iterable[overlay_pb2.ChunkResponse]:
    chunk_index = 0
    while True:
        chunk_resp = stub.GetChunk(overlay_pb2.ChunkRequest(uid=uid, chunk_index=chunk_index))
        if chunk_resp.status != "success":
            print(f"Chunk {chunk_index} fetch terminated with status '{chunk_resp.status}'.")
            break
        yield chunk_resp
        if chunk_resp.is_last:
            break
        chunk_index += 1


def print_chunk_summary(chunk_resp: overlay_pb2.ChunkResponse) -> None:
    rows = json.loads(chunk_resp.data) if chunk_resp.data else []
    if not isinstance(rows, list):
        rows = []
    print(
        f" chunk {chunk_resp.chunk_index+1}/{chunk_resp.total_chunks} "
        f"records={len(rows)} last={chunk_resp.is_last}"
    )


def get_metrics(host: str, port: int) -> None:
    _, channel, stub = _open_stub(host, port)
    try:
        metrics = stub.GetMetrics(overlay_pb2.MetricsRequest())
        print(f"Process {metrics.process_id} ({metrics.role}/{metrics.team})")
        print(f" active_requests={metrics.active_requests} capacity={metrics.max_capacity}")
        print(f" queue_size={metrics.queue_size} avg_ms={metrics.avg_processing_time_ms:.2f}")
        print(f" healthy={metrics.is_healthy} files_loaded={metrics.data_files_loaded}")
    except Exception as exc:
        print(f"Metrics error: {exc}")
    finally:
        channel.close()


def usage() -> None:
    print("Usage: python client.py <host> <port> [metrics|query|date] args...")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        usage()
        sys.exit(1)

    host_arg = sys.argv[1]
    port_arg = int(sys.argv[2])
    command = sys.argv[3] if len(sys.argv) > 3 else "query"

    if command == "metrics":
        get_metrics(host_arg, port_arg)
    elif command == "query":
        if len(sys.argv) < 7:
            print("query command expects: <parameter> <min_value> <max_value>")
            sys.exit(1)
        filters = {
            "parameter": sys.argv[4],
            "min_value": float(sys.argv[5]),
            "max_value": float(sys.argv[6]),
            "limit": 500,
        }
        send_query(host_arg, port_arg, filters)
    elif command == "date":
        if len(sys.argv) < 6:
            print("date command expects: <start_date> <end_date>")
            sys.exit(1)
        filters = {
            "date_start": sys.argv[4],
            "date_end": sys.argv[5],
            "limit": 500,
        }
        send_query(host_arg, port_arg, filters)
    else:
        print(f"Unknown command: {command}")