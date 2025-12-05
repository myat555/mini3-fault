#!/usr/bin/env python3
"""Unified benchmarking tool with real-time visualization of server output."""

import argparse
import json
import os
import sys
import time
import threading
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from collections import defaultdict

import overlay_pb2
import overlay_pb2_grpc
import grpc


class UnifiedBenchmark:
    """Unified benchmark tool."""

    def __init__(
        self,
        leader_host: str,
        leader_port: int,
        config_path: str,
        output_dir: str = "logs",
        query_limit: int = 500,
    ):
        self.leader_host = leader_host
        self.leader_port = leader_port
        self.config_path = config_path
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self._load_config()
        self.query_limit = max(1, query_limit)

    def _load_config(self):
        """Load overlay configuration."""
        with open(self.config_path, "r") as f:
            self.config = json.load(f)
        
        strategies = self.config.get("strategies", {})
        self.fairness_strategy = strategies.get("fairness_strategy", "strict")
        self.strategy_name = f"fairness_{self.fairness_strategy}"

    def collect_process_metrics(self) -> Dict[str, Dict]:
        """Collect metrics from all processes."""
        metrics = {}
        processes = self.config.get("processes", {})
        
        for process_id, process_info in processes.items():
            try:
                address = f"{process_info['host']}:{process_info['port']}"
                with grpc.insecure_channel(address, options=[("grpc.keepalive_timeout_ms", 1000)]) as channel:
                    stub = overlay_pb2_grpc.OverlayNodeStub(channel)
                    try:
                        m = stub.GetMetrics(overlay_pb2.MetricsRequest(), timeout=1)
                        # Try to get strategy fields, with fallback for older proto versions
                        try:
                            fairness_strat = m.fairness_strategy if m.fairness_strategy else "unknown"
                            recent_logs = list(m.recent_logs) if hasattr(m, 'recent_logs') else []
                        except AttributeError:
                            fairness_strat = "unknown"
                            recent_logs = []
                        
                        metrics[process_id] = {
                            "process_id": m.process_id,
                            "role": m.role,
                            "team": m.team,
                            "host": process_info["host"],
                            "port": process_info["port"],
                            "active_requests": m.active_requests,
                            "queue_size": m.queue_size,
                            "avg_processing_time_ms": round(m.avg_processing_time_ms, 2),
                            "data_files_loaded": m.data_files_loaded,
                            "is_healthy": m.is_healthy,
                            "status": "online",
                            "fairness_strategy": fairness_strat,
                            "recent_logs": recent_logs,
                            "timestamp": time.time(),
                        }
                    except grpc.RpcError:
                        metrics[process_id] = {
                            "process_id": process_id,
                            "host": process_info["host"],
                            "status": "offline",
                        }
            except Exception:
                metrics[process_id] = {
                    "process_id": process_id,
                    "status": "offline",
                }
        
        return metrics

    def read_server_logs(self, metrics: Dict, log_dir: Optional[Path] = None, lines: int = 3) -> Dict[str, List[str]]:
        """Read recent server log output from metrics (gRPC) or log files (fallback)."""
        logs = {}
        
        # First, try to get logs from metrics (works across hosts)
        for process_id, proc_metrics in metrics.items():
            if proc_metrics.get("status") == "online" and "recent_logs" in proc_metrics:
                recent_logs = proc_metrics.get("recent_logs", [])
                if recent_logs:
                    logs[process_id] = recent_logs[-lines:] if len(recent_logs) > lines else recent_logs
        
        # Fallback to log files if available (for local processes)
        if log_dir and log_dir.exists():
            processes_config = self.config.get("processes", {})
            for process_id, process_info in processes_config.items():
                if process_id in logs:
                    continue  # Already have logs from metrics
                
                host = process_info.get("host", "")
                proc_lower = process_id.lower()
                
                patterns = [
                    f"*{host}*node_{proc_lower}.log",
                    f"*node_{proc_lower}.log",
                    f"*{proc_lower}*.log",
                    f"*{process_id}*.log",
                    f"macos_*_node_{proc_lower}.log",
                    f"windows_*_node_{proc_lower}.log",
                ]
                
                for pattern in patterns:
                    log_files = list(log_dir.glob(pattern))
                    if log_files:
                        try:
                            log_file = log_files[0]
                            with open(log_file, "r", encoding="utf-8", errors="ignore") as f:
                                all_lines = f.readlines()
                                recent = [line.strip() for line in all_lines[-lines:] if line.strip()]
                                if recent:
                                    logs[process_id] = recent
                                break
                        except Exception:
                            pass
        
        return logs

    def send_query_request(self, query_params: Dict) -> Dict:
        """Send a query request and collect results."""
        try:
            address = f"{self.leader_host}:{self.leader_port}"
            with grpc.insecure_channel(address) as channel:
                stub = overlay_pb2_grpc.OverlayNodeStub(channel)
                
                request = overlay_pb2.QueryRequest(
                    query_type="filter",
                    query_params=json.dumps(query_params),
                    hops=[],
                    client_id="benchmark",
                )
                
                start = time.time()
                response = stub.Query(request)
                latency = (time.time() - start) * 1000
                
                if response.status != "ready" or not response.uid:
                    return {
                        "success": False,
                        "latency": latency,
                        "records": 0,
                        "hops": len(response.hops),
                    }
                
                # Collect all chunks
                total_records = 0
                for chunk_idx in range(response.total_chunks):
                    chunk_resp = stub.GetChunk(
                        overlay_pb2.ChunkRequest(uid=response.uid, chunk_index=chunk_idx)
                    )
                    if chunk_resp.status == "success":
                        try:
                            data = json.loads(chunk_resp.data)
                            total_records += len(data)
                        except:
                            pass
                    if chunk_resp.is_last:
                        break
                
                return {
                    "success": True,
                    "latency": latency,
                    "records": total_records,
                    "hops": len(response.hops),
                }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "latency": 0,
                "records": 0,
            }

    def run_benchmark(
        self,
        num_requests: int = 100,
        concurrency: int = 10,
        update_interval: float = 1.0,
        log_dir: Optional[str] = None,
    ) -> Dict:
        """Run benchmark and output results to file."""
        log_path = Path(log_dir) if log_dir else None
        
        print("=" * 120)
        print("BENCHMARK")
        print("=" * 120)
        print(f"Fairness Strategy: {self.fairness_strategy}")
        print(f"Requests: {num_requests}, Concurrency: {concurrency}")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 120)
        print("Running benchmark...")
        
        # Collect initial metrics
        initial_metrics = self.collect_process_metrics()
        
        results = []
        errors = 0
        lock = threading.Lock()
        
        # Run benchmark
        query_params = {
            "parameter": "PM2.5",
            "min_value": 10.0,
            "max_value": 50.0,
            "limit": self.query_limit,
        }
        
        def worker(worker_id: int, num_per_worker: int):
            nonlocal errors
            local_results = []
            for i in range(num_per_worker):
                result = self.send_query_request(query_params)
                local_results.append(result)
                if not result.get("success"):
                    with lock:
                        errors += 1
                time.sleep(0.01)
            
            with lock:
                results.extend(local_results)
    
        start_time = time.time()
        
        # Start workers
        workers = []
        requests_per_worker = num_requests // concurrency
        for i in range(concurrency):
            worker_id = i
            num_reqs = requests_per_worker + (1 if i < num_requests % concurrency else 0)
            thread = threading.Thread(target=worker, args=(worker_id, num_reqs))
            thread.start()
            workers.append(thread)
        
        # Wait for workers
        for thread in workers:
            thread.join()
        
        duration = time.time() - start_time
        
        # Collect final metrics
        final_metrics = self.collect_process_metrics()
        final_logs = self.read_server_logs(final_metrics, log_path, lines=10)
        
        # Compute final statistics
        latencies = [r["latency"] for r in results if r.get("success")]
        total_records = sum(r.get("records", 0) for r in results)
        successful = sum(1 for r in results if r.get("success"))
        failed = errors
        
        if latencies:
            sorted_latencies = sorted(latencies)
            statistics = {
                "success_rate": (successful / len(results) * 100) if results else 0,
                "avg_latency_ms": sum(latencies) / len(latencies),
                "min_latency_ms": min(latencies),
                "max_latency_ms": max(latencies),
                "p95_latency_ms": sorted_latencies[int(len(sorted_latencies) * 0.95)] if len(sorted_latencies) > 0 else 0,
                "p99_latency_ms": sorted_latencies[int(len(sorted_latencies) * 0.99)] if len(sorted_latencies) > 0 else 0,
                "throughput_req_per_sec": len(results) / duration if duration > 0 else 0,
                "total_records_returned": total_records,
                "avg_records_per_query": total_records / successful if successful > 0 else 0,
            }
        else:
            statistics = {}
        
        benchmark_results = {
            "total_requests": len(results),
            "successful_requests": successful,
            "failed_requests": failed,
            "duration_seconds": duration,
            "statistics": statistics,
            "final_metrics": final_metrics,
            "timestamp": time.time(),
        }
        
        # Generate output file
        output_file = self.output_dir / f"benchmark_{self.strategy_name}.txt"
        
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("=" * 120 + "\n")
            f.write("BENCHMARK\n")
            f.write("=" * 120 + "\n")
            f.write(f"Fairness Strategy: {self.fairness_strategy}\n")
            f.write(f"Requests: {num_requests}, Concurrency: {concurrency}\n")
            f.write(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 120 + "\n\n")
            
            # Process metrics
            hosts = defaultdict(list)
            for process_id, proc in final_metrics.items():
                host = proc.get("host", "unknown")
                hosts[host].append((process_id, proc))
            
            for host, host_processes in sorted(hosts.items()):
                f.write("-" * 120 + "\n")
                f.write(f"HOST: {host}\n")
                f.write("-" * 120 + "\n")
                f.write(f"{'ID':<4} {'Role':<12} {'Team':<6} {'Status':<8} {'Active':<8} {'Queue':<8} {'Avg(ms)':<10} {'Files':<8} {'State':<15}\n")
                f.write("-" * 120 + "\n")
                
                for process_id, proc in sorted(host_processes):
                    pid = proc.get("process_id", "N/A")
                    role = proc.get("role", "N/A")
                    team = proc.get("team", "N/A")
                    status = proc.get("status", "unknown")
                    active = proc.get("active_requests", 0)
                    queue = proc.get("queue_size", 0)
                    avg_ms = proc.get("avg_processing_time_ms", 0)
                    files = proc.get("data_files_loaded", 0)
                    
                    if active > 0:
                        data_indicator = f"Processing {active}"
                    elif files > 0:
                        data_indicator = "Ready"
                    else:
                        data_indicator = "No Data"
                    
                    f.write(
                        f"{pid:<4} {role:<12} {team:<6} {status:<8} "
                        f"{active:<8} {queue:<8} {avg_ms:<10.2f} {files:<8} {data_indicator:<15}\n"
                    )
                
                if final_logs:
                    f.write(f"\n{'─' * 120}\n")
                    f.write(f"RECENT LOGS ({host}):\n")
                    f.write(f"{'─' * 120}\n")
                    for process_id, proc in sorted(host_processes):
                        if process_id in final_logs:
                            for log_line in final_logs[process_id][-3:]:
                                if len(log_line) > 110:
                                    log_line = log_line[:107] + "..."
                                f.write(f"  {process_id}: {log_line}\n")
            
            # Summary
            f.write(f"\n{'=' * 120}\n")
            f.write("BENCHMARK SUMMARY\n")
            f.write(f"{'=' * 120}\n")
            f.write(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Duration: {duration:.2f} seconds\n\n")
            f.write(f"Total Requests: {len(results)}\n")
            f.write(f"Successful: {successful}\n")
            f.write(f"Failed: {failed}\n")
            f.write(f"Success Rate: {statistics.get('success_rate', 0):.2f}%\n\n")
            f.write("Performance Metrics:\n")
            f.write(f"  Average Latency: {statistics.get('avg_latency_ms', 0):.2f} ms\n")
            f.write(f"  Min Latency: {statistics.get('min_latency_ms', 0):.2f} ms\n")
            f.write(f"  Max Latency: {statistics.get('max_latency_ms', 0):.2f} ms\n")
            f.write(f"  P95 Latency: {statistics.get('p95_latency_ms', 0):.2f} ms\n")
            f.write(f"  P99 Latency: {statistics.get('p99_latency_ms', 0):.2f} ms\n")
            f.write(f"  Throughput: {statistics.get('throughput_req_per_sec', 0):.2f} req/sec\n\n")
            f.write("Data Metrics:\n")
            f.write(f"  Total Records Returned: {statistics.get('total_records_returned', 0)}\n")
            f.write(f"  Average Records per Query: {statistics.get('avg_records_per_query', 0):.2f}\n\n")
            f.write("Final Process Metrics:\n")
            for process_id, metrics in sorted(final_metrics.items()):
                if metrics.get("status") == "online":
                    f.write(f"  {process_id} ({metrics.get('role', 'unknown')}/{metrics.get('team', 'unknown')}): ")
                    f.write(f"Active={metrics.get('active_requests', 0)}, ")
                    f.write(f"Queue={metrics.get('queue_size', 0)}, ")
                    f.write(f"AvgTime={metrics.get('avg_processing_time_ms', 0):.2f}ms, ")
                    f.write(f"Files={metrics.get('data_files_loaded', 0)}\n")
            f.write("=" * 120 + "\n")
        
        print(f"Benchmark completed. Results saved to: {output_file}")
        
        return benchmark_results


def main():
    parser = argparse.ArgumentParser(description="Unified benchmark tool.")
    parser.add_argument(
        "--leader-host",
        default="127.0.0.1",
        help="Leader host address.",
    )
    parser.add_argument(
        "--leader-port",
        type=int,
        default=60051,
        help="Leader port.",
    )
    parser.add_argument(
        "--config",
        default="one_host_config.json",
        help="Overlay configuration file.",
    )
    parser.add_argument(
        "--output-dir",
        default="logs",
        help="Output directory for results.",
    )
    parser.add_argument(
        "--num-requests",
        type=int,
        default=100,
        help="Number of requests.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Concurrency level.",
    )
    parser.add_argument(
        "--log-dir",
        help="Directory containing server log files.",
    )
    parser.add_argument(
        "--query-limit",
        type=int,
        default=5000,
        help="Per-request record limit to apply in benchmark queries.",
    )

    args = parser.parse_args()

    benchmark = UnifiedBenchmark(
        args.leader_host,
        args.leader_port,
        args.config,
        args.output_dir,
        query_limit=args.query_limit,
    )
    
    benchmark.run_benchmark(
        args.num_requests,
        args.concurrency,
        1.0,  # update_interval not used anymore but kept for compatibility
        args.log_dir,
    )


if __name__ == "__main__":
    main()

