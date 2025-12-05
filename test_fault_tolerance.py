#!/usr/bin/env python3
"""Comprehensive fault tolerance testing."""

import sys
import time
import json
import subprocess
import signal
import os
import grpc
import overlay_pb2
import overlay_pb2_grpc
from pathlib import Path


class FaultToleranceTester:
    """Test fault tolerance scenarios."""
    
    def __init__(self, config_path: str, leader_host: str, leader_port: int):
        self.config_path = config_path
        self.leader_host = leader_host
        self.leader_port = leader_port
        self.node_processes = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration."""
        with open(self.config_path, "r") as f:
            self.config = json.load(f)
    
    def start_node(self, process_id: str, dataset_root: str = "datasets/2020-fire/data") -> subprocess.Popen:
        """Start a node process."""
        cmd = [
            sys.executable, "-u", "node.py",
            self.config_path,
            process_id,
            "--dataset-root", dataset_root,
        ]
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        self.node_processes[process_id] = proc
        print(f"[Tester] Started node {process_id} (PID: {proc.pid})")
        time.sleep(2)  # Give it time to start
        return proc
    
    def stop_node(self, process_id: str):
        """Stop a node process."""
        if process_id in self.node_processes:
            proc = self.node_processes[process_id]
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            del self.node_processes[process_id]
            print(f"[Tester] Stopped node {process_id}")
            time.sleep(1)
    
    def send_query(self, query_params: dict) -> dict:
        """Send a query."""
        address = f"{self.leader_host}:{self.leader_port}"
        try:
            with grpc.insecure_channel(address) as channel:
                stub = overlay_pb2_grpc.OverlayNodeStub(channel)
                request = overlay_pb2.QueryRequest(
                    query_type="filter",
                    query_params=json.dumps(query_params),
                    hops=[],
                    client_id="fault_test",
                )
                start = time.time()
                response = stub.Query(request, timeout=10)
                latency = (time.time() - start) * 1000
                return {
                    "success": response.status == "ready",
                    "status": response.status,
                    "latency_ms": latency,
                    "hops": len(response.hops),
                }
        except Exception as e:
            return {"success": False, "error": str(e), "latency_ms": 0}
    
    def get_metrics(self, process_id: str) -> dict:
        """Get metrics from a process."""
        proc_info = self.config["processes"][process_id]
        address = f"{proc_info['host']}:{proc_info['port']}"
        try:
            with grpc.insecure_channel(address) as channel:
                stub = overlay_pb2_grpc.OverlayNodeStub(channel)
                m = stub.GetMetrics(overlay_pb2.MetricsRequest(), timeout=2)
                return {
                    "online": True,
                    "active_requests": m.active_requests,
                    "avg_latency_ms": m.avg_processing_time_ms,
                    "is_healthy": m.is_healthy,
                }
        except:
            return {"online": False}
    
    def test_scenario_1_node_failure(self):
        """Test 1: Node failure and routing around it."""
        print("\n" + "=" * 80)
        print("TEST SCENARIO 1: Node Failure and Recovery")
        print("=" * 80)
        
        # Start all nodes
        print("\n1. Starting all nodes...")
        for pid in ["A", "B", "C", "D", "E", "F"]:
            if pid in self.config["processes"]:
                self.start_node(pid)
        
        time.sleep(3)
        
        # Send queries to establish baseline
        print("\n2. Sending baseline queries...")
        query_params = {"parameter": "PM2.5", "min_value": 10.0, "max_value": 50.0, "limit": 100}
        for i in range(3):
            result = self.send_query(query_params)
            print(f"  Query {i+1}: {result['status']}, latency={result['latency_ms']:.1f}ms")
        
        # Kill a worker node
        print("\n3. Killing worker node C...")
        self.stop_node("C")
        time.sleep(6)  # Wait for health check to detect (check_interval=5s)
        
        # Send queries - should route around C
        print("\n4. Sending queries after failure (should route around C)...")
        for i in range(3):
            result = self.send_query(query_params)
            print(f"  Query {i+1}: {result['status']}, latency={result['latency_ms']:.1f}ms, hops={result.get('hops', 0)}")
        
        # Restart node C
        print("\n5. Restarting node C...")
        self.start_node("C")
        time.sleep(7)  # Wait for recovery detection
        
        # Send queries - should use C again
        print("\n6. Sending queries after recovery (should use C again)...")
        for i in range(3):
            result = self.send_query(query_params)
            print(f"  Query {i+1}: {result['status']}, latency={result['latency_ms']:.1f}ms")
        
        print("\n✓ Test 1 complete")
    
    def test_scenario_2_cascading_failure(self):
        """Test 2: Multiple node failures."""
        print("\n" + "=" * 80)
        print("TEST SCENARIO 2: Cascading Failures")
        print("=" * 80)
        
        # Kill multiple nodes
        print("\n1. Killing nodes C and F...")
        if "C" in self.node_processes:
            self.stop_node("C")
        if "F" in self.node_processes:
            self.stop_node("F")
        time.sleep(6)
        
        # Send queries - should still work with remaining nodes
        print("\n2. Sending queries with multiple failures...")
        query_params = {"parameter": "PM2.5", "min_value": 10.0, "max_value": 50.0, "limit": 100}
        for i in range(3):
            result = self.send_query(query_params)
            print(f"  Query {i+1}: {result['status']}, latency={result['latency_ms']:.1f}ms")
        
        print("\n✓ Test 2 complete")
    
    def test_scenario_3_overload_recovery(self):
        """Test 3: Overload detection and state transitions."""
        print("\n" + "=" * 80)
        print("TEST SCENARIO 3: Overload Detection")
        print("=" * 80)
        
        # Send many queries to create overload
        print("\n1. Sending high load to create overload...")
        query_params = {"parameter": "PM2.5", "min_value": 10.0, "max_value": 50.0, "limit": 1000}
        
        import threading
        results = []
        lock = threading.Lock()
        
        def worker():
            for _ in range(10):
                result = self.send_query(query_params)
                with lock:
                    results.append(result)
        
        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Check metrics for state changes
        print("\n2. Checking metrics for state transitions...")
        metrics = self.get_metrics("A")
        if metrics.get("online"):
            print(f"  Active requests: {metrics['active_requests']}")
            print(f"  Avg latency: {metrics['avg_latency_ms']:.1f}ms")
            print(f"  Is healthy: {metrics['is_healthy']}")
        
        print("\n✓ Test 3 complete")
    
    def cleanup(self):
        """Stop all nodes."""
        print("\nCleaning up...")
        for pid in list(self.node_processes.keys()):
            self.stop_node(pid)


def main():
    if len(sys.argv) < 4:
        print("Usage: python test_fault_tolerance.py <config> <leader-host> <leader-port>")
        print("Example: python test_fault_tolerance.py configs/one_host_config.json 127.0.0.1 60051")
        sys.exit(1)
    
    config_path = sys.argv[1]
    leader_host = sys.argv[2]
    leader_port = int(sys.argv[3])
    
    tester = FaultToleranceTester(config_path, leader_host, leader_port)
    
    try:
        tester.test_scenario_1_node_failure()
        time.sleep(2)
        tester.test_scenario_2_cascading_failure()
        time.sleep(2)
        tester.test_scenario_3_overload_recovery()
    finally:
        tester.cleanup()
    
    print("\n" + "=" * 80)
    print("ALL TESTS COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()

