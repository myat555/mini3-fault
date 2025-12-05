# Distributed Overlay Network System

A gRPC-based distributed system implementing multi-process coordination with automatic data partitioning and configurable fairness strategies.

## Quick Start

### Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Generate gRPC code
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. overlay.proto
```

### Running Nodes

**Two-Host Setup (Recommended):**

**Windows (192.168.1.2) - Run in 3 separate terminals:**
```bash
python -u node.py configs/two_hosts_config.json A
python -u node.py configs/two_hosts_config.json B
python -u node.py configs/two_hosts_config.json D
```

**macOS (192.168.1.1) - Run in 3 separate terminals:**
```bash
python -u node.py configs/two_hosts_config.json C
python -u node.py configs/two_hosts_config.json E
python -u node.py configs/two_hosts_config.json F
```

### Running Benchmark

After all 6 nodes are running:
```bash
python benchmark_unified.py \
  --config configs/two_hosts_config.json \
  --leader-host 192.168.1.2 \
  --leader-port 60051 \
  --num-requests 400 \
  --concurrency 20 \
  --log-dir logs/two_hosts \
  --output-dir logs/two_hosts
```

Results saved to: `logs/two_hosts/benchmark_fairness_<strategy>.txt`

### Testing Individual Queries

```bash
python client.py 192.168.1.2 60051 query PM2.5 10 50
python client.py 192.168.1.2 60051 metrics
```

## Architecture

### Process Topology

```
                A (Leader)
               /         \
          B (Team Leader)  E (Team Leader)
         /    |          |    \
     C (W)  D (W)    F (W)  D (W)
     Green         Pink
```

**Nodes:**
- **A** (Leader/Green): Windows, Port 60051 - Entry point for all queries
- **B** (Team Leader/Green): Windows, Port 60052 - Coordinates Green team
- **C** (Worker/Green): macOS, Port 60053 - Green team worker
- **D** (Worker/Pink): Windows, Port 60054 - Pink team worker
- **E** (Team Leader/Pink): macOS, Port 60055 - Coordinates Pink team
- **F** (Worker/Pink): macOS, Port 60056 - Pink team worker

### Data Distribution

- **Team Green** owns dates: 20200810-20200820
- **Team Pink** owns dates: 20200821-20200924
- Data is automatically split among team members
- Workers get larger slices (weight=3), team leaders get medium (weight=2), leader gets small (weight=1)

### Query Flow

1. Client sends query to **A** (Leader)
2. **A** forwards to **B** and **E** (Team Leaders) - splits query limit 50/50
3. **B** forwards to **C** (Worker) - allocates full limit
4. **E** forwards to **D** and **F** (Workers) - splits limit 50/50
5. Results aggregated and returned in chunks
6. Client retrieves chunks on-demand

## Configuration

### Fairness Strategies

Edit `configs/two_hosts_config.json`:

```json
{
  "strategies": {
    "fairness_strategy": "strict",  // Options: "strict", "weighted", "hybrid"
    "chunk_size": 500
  }
}
```

**Strategies:**
- **`strict`**: Hard per-team limits (default: 60 concurrent requests per team)
- **`weighted`**: Flexible limits based on team load
- **`hybrid`**: Strict when load >80%, weighted when load <80%

### Network Requirements

- Both hosts must be on same subnet (192.168.1.x)
- Windows firewall must allow TCP ports 60051-60056
- Both hosts must be able to ping each other


