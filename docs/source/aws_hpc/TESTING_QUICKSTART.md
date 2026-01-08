# AWS Batch Testing Quick Start

Minimal steps to test AWS Batch worker adapter.

## Prerequisites

- Docker running
- AWS credentials configured
- Python 3.8+

---

## 1. Provision AWS Resources (Host)

**⚠️ IMPORTANT**: Run this on your **host machine** (not in container) where Docker daemon is accessible.

```bash
# On host machine (where Docker is running)
cd /Users/blitvin/IdeaProjects/opengris-scaler

# Create provisioning venv (one-time)
python3 -m venv .venv-provision
.venv-provision/bin/pip install boto3

# Provision (builds Docker image + creates AWS resources)
.venv-provision/bin/python src/scaler/utility/worker_adapter/aws_hpc/provisioner.py provision \
    --region us-east-1 \
    --prefix scaler-batch \
    --vcpus 4 \
    --memory 8192
```

**Output**: Creates `.scaler_aws_hpc.env` in project root with configuration.

**Why host?** The provisioner needs Docker to build/push the container image to ECR. Dev containers typically don't have Docker-in-Docker.

---

## 2. Start Dev Container

### Option A: Static credentials (~/.aws/credentials)

```bash
docker run -it --rm \
    -v ~/.aws:/root/.aws:ro \
    -v $(pwd):/workspace -w /workspace \
    scaler-dev bash
```

### Option B: Temporary credentials (SSO/assumed role)

```bash
# Export credentials first
eval $(aws configure export-credentials --format env)

# Start container
docker run -it --rm \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e AWS_SESSION_TOKEN \
    -e AWS_DEFAULT_REGION=us-east-1 \
    -v $(pwd):/workspace -w /workspace \
    scaler-dev bash
```

---

## 3. Install & Run (Inside Container)

```bash
# Install
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]" boto3

# Load AWS config
source .scaler_aws_hpc.env

# Start scheduler (background)
python -c "
import sys
sys.argv = ['scheduler', 'tcp://0.0.0.0:2345']
from scaler.entry_points.scheduler import main
main()
" &

sleep 2

# Start AWS Batch worker (background)
python -m scaler.entry_points.worker_adapter_aws_hpc \
    --scheduler-address tcp://127.0.0.1:2345 \
    --job-queue $SCALER_JOB_QUEUE \
    --job-definition $SCALER_JOB_DEFINITION \
    --s3-bucket $SCALER_S3_BUCKET \
    --aws-region $SCALER_AWS_REGION \
    --max-concurrent-jobs 100 \
    --log-level INFO &

# Run tests
python tests/worker_adapter/aws_hpc/aws_hpc_test_harness.py \
    --scheduler tcp://127.0.0.1:2345 \
    --test all
```

**Expected output**:
```
==================================================
AWS HPC Worker Adapter Test Harness
==================================================
Scheduler: tcp://127.0.0.1:2345
Connected to scheduler

--- Test: sqrt ---
  Submitting: math.sqrt(16)
  Result: 4.0
  PASSED

--- Test: simple ---
  Submitting: simple_task(21) [returns x * 2]
  Result: 42
  PASSED

--- Test: map ---
  Submitting: client.map(simple_task, [0,1,2,3,4])
  Results: [0, 2, 4, 6, 8]
  PASSED

--- Test: compute ---
  Submitting: compute_task(1000)
  Result: 3328335.00
  PASSED

==================================================
Results: 4/4 passed
```

---

## 4. Run Individual Tests

```bash
# Single test
python tests/worker_adapter/aws_hpc/aws_hpc_test_harness.py \
    --scheduler tcp://127.0.0.1:2345 \
    --test sqrt

# Custom timeout (default: 300s)
python tests/worker_adapter/aws_hpc/aws_hpc_test_harness.py \
    --scheduler tcp://127.0.0.1:2345 \
    --test all \
    --timeout 600
```

---

## 5. Manual Testing

```python
from scaler import Client

with Client(address="tcp://127.0.0.1:2345") as client:
    # Single task
    future = client.submit(lambda x: x * 2, 21)
    print(future.result())  # 42
    
    # Multiple tasks
    results = client.map(lambda x: x * 2, [(i,) for i in range(5)])
    print(results)  # [0, 2, 4, 6, 8]
```

---

## 6. Cleanup

```bash
# Stop background processes (inside container)
pkill -f "scaler.entry_points"

# Exit container
exit

# Delete AWS resources (on host)
.venv-provision/bin/python src/scaler/utility/worker_adapter/aws_hpc/provisioner.py cleanup \
    --region us-east-1 \
    --prefix scaler-batch
```

---

## Troubleshooting

### Test timeout
- **Cause**: EC2 cold start takes 2-3 minutes
- **Fix**: Increase timeout: `--timeout 600`

### Credential errors
- **Cause**: Session expired (SSO/assumed role)
- **Fix**: Exit container, re-export credentials, restart

### Job failures
- **Check logs**: AWS Console → Batch → Jobs → View logs
- **Check S3**: Verify task data uploaded to `s3://$SCALER_S3_BUCKET/scaler-tasks/`

### Worker not connecting
- **Check scheduler**: Should see "Worker connected" in logs
- **Check ports**: Scheduler binds to `0.0.0.0:2345`

---

## Test Coverage

| Test | Function | Expected Result |
|------|----------|-----------------|
| `sqrt` | `math.sqrt(16)` | `4.0` |
| `simple` | `simple_task(21)` | `42` |
| `map` | `client.map(simple_task, [0..4])` | `[0, 2, 4, 6, 8]` |
| `compute` | `compute_task(1000)` | `~3328335.0` |

---

## Architecture

```
Client → Scheduler → AWSBatchWorker → AWS Batch → EC2 Container
                          ↓                ↓
                    Object Storage      S3 Bucket
```

**Key Components**:
- **Scheduler**: Routes tasks to workers
- **AWSBatchWorker**: Submits jobs to AWS Batch, polls for results
- **AWS Batch**: Manages job queue and EC2 compute environment
- **S3**: Stores large task payloads and results
- **Object Storage**: Stores function/args/results for Scaler protocol

---

## Next Steps

- See [AWS_BATCH_SETUP.md](AWS_BATCH_SETUP.md) for detailed setup
- See [design.md](design.md) for architecture details
- See [test_plan.md](test_plan.md) for comprehensive testing strategy
