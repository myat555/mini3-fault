import unittest
from unittest.mock import MagicMock, patch
import time
import grpc
from overlay_core.resilience import retry, CircuitBreaker
from overlay_core.proxies import RemoteNodeClient
from overlay_core.config import ProcessSpec
import overlay_pb2

class TestResilience(unittest.TestCase):
    def test_retry_eventually_succeeds(self):
        print("\nTesting Retry: Eventually Succeeds")
        mock_func = MagicMock(side_effect=[ValueError("Fail 1"), ValueError("Fail 2"), "Success"])
        
        @retry(max_retries=3, initial_delay=0.01, exceptions=(ValueError,))
        def decorated_func():
            return mock_func()
            
        result = decorated_func()
        self.assertEqual(result, "Success")
        self.assertEqual(mock_func.call_count, 3)
        print("  -> Function called 3 times as expected (2 fails + 1 success)")

    def test_retry_max_retries_exceeded(self):
        print("\nTesting Retry: Max Retries Exceeded")
        mock_func = MagicMock(side_effect=ValueError("Fail"))
        
        @retry(max_retries=2, initial_delay=0.01, exceptions=(ValueError,))
        def decorated_func():
            return mock_func()
            
        with self.assertRaises(ValueError):
            decorated_func()
        
        # Initial call + 2 retries = 3 calls
        self.assertEqual(mock_func.call_count, 3)
        print("  -> Function called 3 times (initial + 2 retries) before giving up")

    def test_circuit_breaker_trips(self):
        print("\nTesting Circuit Breaker: Trips on Failure")
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1.0, exceptions=(ValueError,))
        
        # Fail twice to trip
        cb.record_failure()
        cb.record_failure()
        
        self.assertFalse(cb.allow_request())
        print("  -> Circuit breaker is OPEN after 2 failures")
        
        # Verify it raises when checked manually
        with self.assertRaises(Exception) as cm:
            if not cb.allow_request():
                raise Exception("CircuitBreaker is OPEN")
        self.assertIn("CircuitBreaker is OPEN", str(cm.exception))

    def test_circuit_breaker_recovery(self):
        print("\nTesting Circuit Breaker: Recovery")
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1, exceptions=(ValueError,))
        cb.record_failure()
        self.assertFalse(cb.allow_request())
        
        # Wait for recovery timeout
        time.sleep(0.15)
        
        # Should be half-open (allowed)
        self.assertTrue(cb.allow_request())
        print("  -> Circuit breaker is HALF-OPEN after timeout")
        
        # Success closes it
        cb.record_success()
        self.assertTrue(cb.allow_request())
        print("  -> Circuit breaker is CLOSED after success")

class TestRemoteNodeClientIntegration(unittest.TestCase):
    def setUp(self):
        # Mock ProcessSpec since we don't want to load real config
        self.spec = MagicMock(spec=ProcessSpec)
        self.spec.address = "localhost:50051"
        
        self.channel = MagicMock(spec=grpc.Channel)
        self.client = RemoteNodeClient(self.spec, self.channel)
        
        # Mock the stub attached to the client
        self.client._stub = MagicMock()

    def test_query_retries_on_rpc_error(self):
        print("\nTesting Integration: Client Retries on RPC Error")
        # Fail twice, then succeed
        self.client._stub.Query.side_effect = [
            grpc.RpcError("Transient 1"),
            grpc.RpcError("Transient 2"),
            "SuccessResponse"
        ]
        
        response = self.client.query(overlay_pb2.QueryRequest())
        self.assertEqual(response, "SuccessResponse")
        self.assertEqual(self.client._stub.Query.call_count, 3)
        print("  -> Client retried 2 times and succeeded on 3rd attempt")

    def test_circuit_breaker_prevents_calls(self):
        print("\nTesting Integration: Circuit Breaker Prevents Calls")
        # Force breaker open
        for _ in range(5):
            self.client._circuit_breaker.record_failure()
            
        # Next call should fail fast without touching stub
        with self.assertRaises(RuntimeError) as cm:
            self.client.query(overlay_pb2.QueryRequest())
            
        self.assertIn("CircuitBreaker is OPEN", str(cm.exception))
        self.client._stub.Query.assert_not_called()
        print("  -> Client raised RuntimeError immediately, no RPC call made")

if __name__ == '__main__':
    unittest.main()
