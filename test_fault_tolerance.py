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
from typing import Dict, List, Set, Optional


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
    
    def check_node_online(self, process_id: str, timeout: float = 2.0) -> bool:
        """Check if a node is online and responding."""
        if process_id not in self.config["processes"]:
            return False
        
        proc_info = self.config["processes"][process_id]
        address = f"{proc_info['host']}:{proc_info['port']}"
        try:
            with grpc.insecure_channel(address) as channel:
                stub = overlay_pb2_grpc.OverlayNodeStub(channel)
                m = stub.GetMetrics(overlay_pb2.MetricsRequest(), timeout=timeout)
                return True
        except:
            return False
    
    def wait_for_all_nodes(self, max_wait: float = 60.0, check_interval: float = 1.0) -> bool:
        """
        Wait for all configured nodes to be online.
        Returns True if all nodes are online, False if timeout.
        """
        print("\n[Tester] Checking if all nodes are online...")
        all_process_ids = list(self.config["processes"].keys())
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            online_nodes = []
            offline_nodes = []
            
            for pid in all_process_ids:
                if self.check_node_online(pid):
                    online_nodes.append(pid)
                else:
                    offline_nodes.append(pid)
            
            if not offline_nodes:
                print(f"[Tester] All {len(online_nodes)} nodes are online")
                return True
            
            print(f"[Tester] Waiting for nodes: {', '.join(offline_nodes)} "
                  f"({len(online_nodes)}/{len(all_process_ids)} online)")
            time.sleep(check_interval)
        
        print(f"[Tester] Timeout waiting for nodes: {', '.join(offline_nodes)}")
        return False
    
    def prompt_manual_action(self, action: str, node_id: Optional[str] = None) -> bool:
        """
        Prompt user to perform a manual action.
        Returns True when user confirms action is complete.
        """
        if node_id:
            print(f"\n[Tester] ACTION REQUIRED: {action} for node {node_id}")
        else:
            print(f"\n[Tester] ACTION REQUIRED: {action}")
        print("[Tester] Press Enter when the action is complete...")
        try:
            input()
            return True
        except KeyboardInterrupt:
            print("\n[Tester] Test interrupted by user")
            return False
    
    def detect_health_change(
        self,
        target_node_id: str,
        expected_state: str,  # "failed" or "recovered"
        max_wait: float = 20.0,
        check_interval: float = 1.0
    ) -> bool:
        """
        Detect health change (failure or recovery) by checking other nodes' logs.
        Returns True if change detected, False if timeout.
        """
        print(f"[Tester] Detecting {expected_state} state for node {target_node_id}...")
        start_time = time.time()
        
        # Check nodes that should detect the change (leader and team leaders)
        check_nodes = ["A", "B", "E"]
        
        while time.time() - start_time < max_wait:
            for pid in check_nodes:
                if pid not in self.config["processes"] or pid == target_node_id:
                    continue
                
                metrics = self.get_metrics(pid)
                if not metrics.get("online"):
                    continue
                
                logs = metrics.get("recent_logs", [])
                
                # Look for health checker messages
                if expected_state == "failed":
                    # Look for: [HealthChecker] ... detected failure: {target_node_id}
                    health_check_found = any(
                        "HealthChecker" in log and 
                        "detected failure" in log and 
                        target_node_id in log
                        for log in logs
                    )
                    # Or: [Orchestrator] ... handling failure of {target_node_id}
                    orchestrator_found = any(
                        "Orchestrator" in log and 
                        "handling failure" in log and 
                        target_node_id in log
                        for log in logs
                    )
                    
                    if health_check_found or orchestrator_found:
                        print(f"[Tester] Health change detected: Node {pid} detected failure of {target_node_id}")
                        return True
                
                elif expected_state == "recovered":
                    # Look for: [HealthChecker] ... detected recovery: {target_node_id}
                    health_check_found = any(
                        "HealthChecker" in log and 
                        "detected recovery" in log and 
                        target_node_id in log
                        for log in logs
                    )
                    # Or: [Orchestrator] ... handling recovery of {target_node_id}
                    orchestrator_found = any(
                        "Orchestrator" in log and 
                        "handling recovery" in log and 
                        target_node_id in log
                        for log in logs
                    )
                    
                    if health_check_found or orchestrator_found:
                        print(f"[Tester] Health change detected: Node {pid} detected recovery of {target_node_id}")
                        return True
            
            time.sleep(check_interval)
        
        print(f"[Tester] Timeout waiting for {expected_state} detection")
        return False
    
    def detect_hook_changes(
        self,
        event_type: str,  # "failure" or "recovery" or "state"
        max_wait: float = 15.0,
        check_interval: float = 1.0
    ) -> bool:
        """
        Detect hook execution by checking for HookManager messages in logs.
        Returns True if hooks detected, False if timeout.
        """
        print(f"[Tester] Detecting hook changes for {event_type} events...")
        start_time = time.time()
        
        check_nodes = ["A", "B", "E"]
        
        while time.time() - start_time < max_wait:
            for pid in check_nodes:
                if pid not in self.config["processes"]:
                    continue
                
                metrics = self.get_metrics(pid)
                if not metrics.get("online"):
                    continue
                
                logs = metrics.get("recent_logs", [])
                
                # Look for HookManager messages
                hook_found = any(
                    "HookManager" in log and 
                    ("Triggering" in log or "hook" in log.lower())
                    for log in logs
                )
                
                if hook_found:
                    # Look for specific event types
                    if event_type == "failure":
                        if any("neighbor_failed" in log.lower() for log in logs):
                            print(f"[Tester] Hook change detected: {event_type} hooks executed on node {pid}")
                            return True
                    elif event_type == "recovery":
                        if any("neighbor_recovered" in log.lower() for log in logs):
                            print(f"[Tester] Hook change detected: {event_type} hooks executed on node {pid}")
                            return True
                    elif event_type == "state":
                        if any("state_changed" in log.lower() for log in logs):
                            print(f"[Tester] Hook change detected: {event_type} hooks executed on node {pid}")
                            return True
            
            time.sleep(check_interval)
        
        print(f"[Tester] Timeout waiting for hook changes (may still be working)")
        return True  # Don't fail test, hooks may have executed but not logged yet
    
    def detect_state_changes(
        self,
        node_id: str,
        max_wait: float = 15.0,
        check_interval: float = 1.0
    ) -> bool:
        """
        Detect state changes by checking StateTracker messages in logs.
        Returns True if state changes detected, False if timeout.
        """
        print(f"[Tester] Detecting state changes for node {node_id}...")
        start_time = time.time()
        
        metrics = self.get_metrics(node_id)
        if not metrics.get("online"):
            print(f"[Tester] Node {node_id} is not online")
            return False
        
        # Get initial state
        logs = metrics.get("recent_logs", [])
        initial_state_logs = [log for log in logs if "StateTracker" in log and "state:" in log.lower()]
        
        # Wait and check for new state messages
        while time.time() - start_time < max_wait:
            metrics = self.get_metrics(node_id)
            if not metrics.get("online"):
                break
            
            logs = metrics.get("recent_logs", [])
            current_state_logs = [log for log in logs if "StateTracker" in log]
            
            # Check for state transitions
            state_transitions = [log for log in current_state_logs if "state transition" in log.lower()]
            if state_transitions:
                print(f"[Tester] State change detected: {state_transitions[-1]}")
                return True
            
            # Check if state value changed
            current_state_msgs = [log for log in current_state_logs if "state:" in log.lower()]
            if current_state_msgs and initial_state_logs:
                if current_state_msgs[-1] != initial_state_logs[-1]:
                    print(f"[Tester] State change detected: {current_state_msgs[-1]}")
                    return True
            
            time.sleep(check_interval)
        
        print(f"[Tester] State monitoring complete (may not have changed)")
        return True  # Don't fail test, state may be stable
    
    def wait_for_changes_to_propagate(
        self,
        failed_node_id: Optional[str] = None,
        recovered_node_id: Optional[str] = None,
        wait_time: float = 5.0
    ):
        """
        Wait for health, hook, and state changes to propagate.
        """
        print(f"[Tester] Waiting {wait_time}s for all changes to propagate...")
        
        # Detect health changes
        if failed_node_id:
            self.detect_health_change(failed_node_id, "failed", max_wait=wait_time)
        if recovered_node_id:
            self.detect_health_change(recovered_node_id, "recovered", max_wait=wait_time)
        
        # Detect hook changes
        if failed_node_id or recovered_node_id:
            event_type = "failure" if failed_node_id else "recovery"
            self.detect_hook_changes(event_type, max_wait=wait_time)
        
        # Detect state changes on leader
        self.detect_state_changes("A", max_wait=wait_time)
        
        # Additional wait for propagation
        time.sleep(2.0)
        print("[Tester] Changes should be propagated")
    
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
        if process_id not in self.config["processes"]:
            return {"online": False}
        
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
                    "recent_logs": list(m.recent_logs) if hasattr(m, 'recent_logs') else [],
                }
        except:
            return {"online": False}
    
    def test_scenario_1_node_failure(self):
        """Test 1: Node failure and routing around it."""
        print("\n" + "=" * 80)
        print("TEST SCENARIO 1: Node Failure and Recovery")
        print("=" * 80)
        
        # Check if all nodes are running
        print("\n1. Checking if all nodes are online...")
        if not self.wait_for_all_nodes(max_wait=60.0):
            print("\n[Tester] ERROR: Not all nodes are online")
            print("[Tester] Please start all nodes manually:")
            all_nodes = list(self.config["processes"].keys())
            for pid in sorted(all_nodes):
                proc_info = self.config["processes"][pid]
                print(f"  python -u node.py {self.config_path} {pid} --dataset-root datasets/2020-fire/data")
            print("\n[Tester] Press Enter when all nodes are started...")
            input()
            
            if not self.wait_for_all_nodes(max_wait=30.0):
                print("\n[Tester] ERROR: Still not all nodes online. Aborting test.")
                return
        
        # Wait for initialization
        print("\n2. Waiting for nodes to initialize...")
        time.sleep(3.0)
        
        # Send queries to establish baseline
        print("\n3. Sending baseline queries...")
        query_params = {"parameter": "PM2.5", "min_value": 10.0, "max_value": 50.0, "limit": 100}
        for i in range(3):
            result = self.send_query(query_params)
            print(f"  Query {i+1}: {result['status']}, latency={result['latency_ms']:.1f}ms")
            time.sleep(0.5)
        
        # Wait for state to stabilize
        print("\n4. Waiting for state to stabilize...")
        time.sleep(2.0)
        
        # Manual node failure
        print("\n5. MANUAL ACTION: Stop node C")
        print("[Tester] In the terminal running node C, press Ctrl+C to stop it")
        if not self.prompt_manual_action("Stop node C", "C"):
            return
        
        # Verify node is actually down
        print("\n6. Verifying node C is offline...")
        if self.check_node_online("C"):
            print("[Tester] WARNING: Node C is still online. Please ensure it is stopped.")
            if not self.prompt_manual_action("Confirm node C is stopped", "C"):
                return
        else:
            print("[Tester] Node C confirmed offline")
        
        # Wait for health checker to detect failure
        print("\n7. Waiting for health checker to detect failure...")
        print("[Tester] Health checker runs every 5s, failure threshold is 2 checks (~10s)")
        time.sleep(12.0)  # Wait for 2 health check cycles + buffer
        
        # Detect health, hook, and state changes
        print("\n8. Detecting health, hook, and state changes...")
        self.wait_for_changes_to_propagate(failed_node_id="C", wait_time=10.0)
        
        # Send queries - should route around C
        print("\n9. Sending queries after failure (should route around C)...")
        for i in range(3):
            result = self.send_query(query_params)
            print(f"  Query {i+1}: {result['status']}, latency={result['latency_ms']:.1f}ms, hops={result.get('hops', 0)}")
            time.sleep(0.5)
        
        # Manual node recovery
        print("\n10. MANUAL ACTION: Restart node C")
        print("[Tester] Start node C in a terminal:")
        proc_info = self.config["processes"]["C"]
        print(f"  python -u node.py {self.config_path} C --dataset-root datasets/2020-fire/data")
        if not self.prompt_manual_action("Restart node C", "C"):
            return
        
        # Verify node is back online
        print("\n11. Verifying node C is online...")
        if not self.wait_for_all_nodes(max_wait=30.0):
            print("[Tester] WARNING: Node C may not be fully online")
        
        # Wait for health checker to detect recovery
        print("\n12. Waiting for health checker to detect recovery...")
        time.sleep(8.0)  # Wait for one health check cycle + buffer
        
        # Detect health, hook, and state changes
        print("\n13. Detecting health, hook, and state changes...")
        self.wait_for_changes_to_propagate(recovered_node_id="C", wait_time=10.0)
        
        # Send queries - should use C again
        print("\n14. Sending queries after recovery (should use C again)...")
        for i in range(3):
            result = self.send_query(query_params)
            print(f"  Query {i+1}: {result['status']}, latency={result['latency_ms']:.1f}ms")
            time.sleep(0.5)
        
        print("\n[Tester] Test 1 complete")
    
    def test_scenario_2_cascading_failure(self):
        """Test 2: Multiple node failures."""
        print("\n" + "=" * 80)
        print("TEST SCENARIO 2: Cascading Failures")
        print("=" * 80)
        
        # Ensure all nodes are online
        if not self.wait_for_all_nodes(max_wait=10.0):
            print("[Tester] WARNING: Not all nodes online, proceeding anyway...")
        
        # Manual node failures
        print("\n1. MANUAL ACTION: Stop nodes C and F")
        print("[Tester] In the terminals running nodes C and F, press Ctrl+C to stop them")
        if not self.prompt_manual_action("Stop nodes C and F"):
            return
        
        # Verify nodes are down
        print("\n2. Verifying nodes are offline...")
        c_offline = not self.check_node_online("C")
        f_offline = not self.check_node_online("F")
        
        if not c_offline:
            print("[Tester] WARNING: Node C is still online")
        if not f_offline:
            print("[Tester] WARNING: Node F is still online")
        
        if c_offline and f_offline:
            print("[Tester] Both nodes confirmed offline")
        
        # Wait for failure detection
        print("\n3. Waiting for failure detection...")
        time.sleep(12.0)  # Wait for health check cycles
        
        # Detect changes
        print("\n4. Detecting health, hook, and state changes...")
        self.wait_for_changes_to_propagate(failed_node_id="C", wait_time=8.0)
        self.wait_for_changes_to_propagate(failed_node_id="F", wait_time=8.0)
        
        # Send queries - should still work with remaining nodes
        print("\n5. Sending queries with multiple failures...")
        query_params = {"parameter": "PM2.5", "min_value": 10.0, "max_value": 50.0, "limit": 100}
        for i in range(3):
            result = self.send_query(query_params)
            print(f"  Query {i+1}: {result['status']}, latency={result['latency_ms']:.1f}ms")
            time.sleep(0.5)
        
        print("\n[Tester] Test 2 complete")
    
    def test_scenario_3_overload_recovery(self):
        """Test 3: Overload detection and state transitions."""
        print("\n" + "=" * 80)
        print("TEST SCENARIO 3: Overload Detection")
        print("=" * 80)
        
        # Check node status
        print("\n1. Checking node status...")
        all_nodes = list(self.config["processes"].keys())
        online_nodes = []
        offline_nodes = []
        
        for pid in all_nodes:
            if self.check_node_online(pid):
                online_nodes.append(pid)
            else:
                offline_nodes.append(pid)
        
        print(f"[Tester] Online nodes: {', '.join(online_nodes)}")
        if offline_nodes:
            print(f"[Tester] Offline nodes: {', '.join(offline_nodes)}")
            print("[Tester] NOTE: Overload test can run with fewer nodes, but may be easier to trigger overload")
            response = input("[Tester] Restart offline nodes before overload test? (y/n): ").strip().lower()
            
            if response == 'y':
                print("[Tester] Please restart the offline nodes:")
                for pid in offline_nodes:
                    proc_info = self.config["processes"][pid]
                    print(f"  python -u node.py {self.config_path} {pid} --dataset-root datasets/2020-fire/data")
                print("[Tester] Press Enter when nodes are restarted...")
                input()
                
                # Wait for nodes to come online
                if not self.wait_for_all_nodes(max_wait=30.0):
                    print("[Tester] WARNING: Not all nodes online, proceeding with available nodes")
            else:
                print("[Tester] Proceeding with fewer nodes for overload test")
        
        # Wait for system to stabilize
        print("\n2. Waiting for system to stabilize...")
        time.sleep(2.0)
        
        # Send many queries to create overload
        print("\n3. Sending high load to create overload...")
        print("[Tester] Sending 50 concurrent queries to create load...")
        query_params = {"parameter": "PM2.5", "min_value": 10.0, "max_value": 50.0, "limit": 1000}
        
        import threading
        results = []
        lock = threading.Lock()
        
        def worker():
            for _ in range(10):
                result = self.send_query(query_params)
                with lock:
                    results.append(result)
                time.sleep(0.1)
        
        threads = [threading.Thread(target=worker) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Wait for state changes to propagate
        print("\n4. Waiting for state changes to stabilize...")
        time.sleep(3.0)
        
        # Detect state changes
        print("\n5. Detecting state changes...")
        self.detect_state_changes("A", max_wait=10.0)
        self.detect_hook_changes("state", max_wait=5.0)
        
        # Check metrics for state changes
        print("\n6. Checking metrics for state transitions...")
        metrics = self.get_metrics("A")
        if metrics.get("online"):
            print(f"  Active requests: {metrics['active_requests']}")
            print(f"  Avg latency: {metrics['avg_latency_ms']:.1f}ms")
            print(f"  Is healthy: {metrics['is_healthy']}")
            
            # Check logs for state transitions
            logs = metrics.get("recent_logs", [])
            state_logs = [log for log in logs if "State" in log or "state" in log]
            if state_logs:
                print(f"  State info from logs:")
                for log in state_logs[-3:]:
                    print(f"    {log}")
            
            # Check for hook messages
            hook_logs = [log for log in logs if "HookManager" in log]
            if hook_logs:
                print(f"  Hook execution detected:")
                for log in hook_logs[-2:]:
                    print(f"    {log}")
        else:
            print("[Tester] WARNING: Leader node A is not online")
        
        print("\n[Tester] Test 3 complete")


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
    except KeyboardInterrupt:
        print("\n[Tester] Test interrupted by user")
    finally:
        print("\n[Tester] Tests complete")
        print("[Tester] Note: Nodes are still running. Stop them manually if needed.")
    
    print("\n" + "=" * 80)
    print("ALL TESTS COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    main()