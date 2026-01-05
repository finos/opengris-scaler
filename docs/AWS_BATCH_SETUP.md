# AWS Batch Worker Adapter Setup Guide

This guide walks you through setting up and testing the AWS Batch worker adapter for Scaler.

## Architecture

```
┌─────────┐     ┌───────────┐     ┌─────────────────┐     ┌───────────┐
│  Client │────▶│ Scheduler │────▶│ AWSBatchWorker  │────▶│ AWS Batch │
└─────────┘     └───────────┘     └─────────────────┘     └───────────┘
                                          │                      │
                                          ▼                      ▼
                                    ┌──────────┐          ┌──────────┐
                                    │ S3 Bucket│◀─────────│ Batch Job│
                                    └──────────┘          └──────────┘
```

## Prerequisites

- Docker (for devcontainer)
- AWS Account with permissions for:
  - S3 (create bucket, read/write objects)
  - IAM (create roles and policies)
  - AWS Batch (create compute environments, job queues, job definitions)
- AWS CLI configured with credentials

---

## Step 1: Start Development Container

```bash
# Build the development container
docker build -t scaler-dev -f .devcontainer/Dockerfile .

# Run the container with workspace mounted
docker run -it -v $(pwd):/workspace -w /workspace scaler-dev bash
```

## Step 2: Install Dependencies

```bash
# Inside the container
cd /workspace

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install scaler in development mode
pip install -e ".[dev]"

# Install AWS dependencies
pip install boto3
```

## Step 3: Configure AWS Credentials

```bash
# Option 1: Use AWS CLI
aws configure
# Enter your AWS Access Key ID, Secret Access Key, and region

# Option 2: Export environment variables
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"
```

## Step 4: Provision AWS Resources

```bash
# Provision S3 bucket, IAM role, Batch compute environment, job queue, and job definition
python -m scaler.worker_adapter.aws_batch.provisioner provision \
    --region us-east-1 \
    --prefix scaler-batch \
    --image python:3.11-slim \
    --vcpus 1 \
    --memory 1843
```

Copy and run the export commands printed at the end:
```bash
export SCALER_AWS_REGION="us-east-1"
export SCALER_S3_BUCKET="scaler-batch-123456789012-us-east-1"
export SCALER_JOB_QUEUE="scaler-batch-queue"
export SCALER_JOB_DEFINITION="scaler-batch-job"
```

**Note on memory:** For Fargate, use ~90% of the desired memory if it's a multiple of 2048 MB.
This reserves headroom for container overhead.

| Desired Memory | Recommended `--memory` |
|----------------|------------------------|
| 2048 MB (2 GB) | 1843 |
| 4096 MB (4 GB) | 3686 |
| 8192 MB (8 GB) | 7372 |

Note the output - you'll need the bucket name, job queue, and job definition names.

## Step 5: Start the Scheduler

Open a new terminal:

```bash
source .venv/bin/activate

# Start scheduler on default port 2345
python -m scaler.entry_points.scheduler \
    --address tcp://0.0.0.0:2345
```

## Step 6: Start the AWS Batch Worker

Open another terminal:

```bash
source .venv/bin/activate

# If you haven't set the env vars from Step 4, run:
# python -m scaler.worker_adapter.aws_batch.provisioner show

# Start AWS Batch worker adapter
python -m scaler.entry_points.worker_adapter_aws_batch \
    --scheduler-address tcp://127.0.0.1:2345 \
    --job-queue $SCALER_JOB_QUEUE \
    --job-definition $SCALER_JOB_DEFINITION \
    --s3-bucket $SCALER_S3_BUCKET \
    --aws-region $SCALER_AWS_REGION \
    --max-concurrent-jobs 10 \
    --log-level INFO
```

## Step 7: Run Tests

Open another terminal:

```bash
source .venv/bin/activate

# Run test harness
python tests/aws_batch_test_harness.py \
    --scheduler tcp://127.0.0.1:2345 \
    --test all
```

Expected output:
```
==================================================
AWS Batch Worker Adapter Test Harness
==================================================
Scheduler: tcp://127.0.0.1:2345
Connected to scheduler

--- Test: Simple Task ---
  Result: 42
  PASSED

--- Test: Map Tasks ---
  Results: [0, 2, 4, 6, 8]
  PASSED

--- Test: Compute Task ---
  Result: 666616.46
  PASSED

==================================================
Results: 3/3 passed
```

## Step 8: Use in Your Code

```python
from scaler import Client

# Connect to scheduler
with Client(address="tcp://127.0.0.1:2345") as client:
    # Submit a single task
    future = client.submit(my_function, arg1, arg2)
    result = future.result()

    # Submit multiple tasks
    futures = client.map(my_function, [(arg1,), (arg2,), (arg3,)])
    results = [f.result() for f in futures]
```

---

## Cleanup

When done, clean up AWS resources:

```bash
python -m scaler.worker_adapter.aws_batch.provisioner cleanup \
    --region us-east-1 \
    --prefix scaler-batch
```

---

## Troubleshooting

### "No scaler job queue found"

Run the provisioner:
```bash
python -m scaler.worker_adapter.aws_batch.provisioner provision
```

### "Failed to connect to scheduler"

1. Check scheduler is running: `ps aux | grep scheduler`
2. Check address is correct
3. Check firewall/network settings

### "AWS Batch job failed"

1. Check CloudWatch Logs for the job
2. Verify IAM role has S3 permissions
3. Check S3 bucket exists and is accessible

### "Timeout waiting for result"

1. Check AWS Batch console for job status
2. Increase `--max-concurrent-jobs` if jobs are queued
3. Check compute environment has capacity

### Container image issues

Make sure your job definition uses an image with:
- Python 3.8+
- `cloudpickle` and `boto3` installed

Custom image example:
```dockerfile
FROM python:3.11-slim
RUN pip install cloudpickle boto3
COPY . /app
WORKDIR /app
RUN pip install -e .
```

---

## Configuration Reference

### Provisioner Options

| Option | Default | Description |
|--------|---------|-------------|
| `--region` | us-east-1 | AWS region |
| `--prefix` | scaler-batch | Resource name prefix |
| `--image` | python:3.11-slim | Container image |
| `--vcpus` | 1.0 | vCPUs per job |
| `--memory` | 2048 | Memory per job (MB) |
| `--max-vcpus` | 256 | Max vCPUs for compute env |

### Worker Options

| Option | Default | Description |
|--------|---------|-------------|
| `--scheduler-address` | required | Scheduler address |
| `--job-queue` | required | AWS Batch job queue |
| `--job-definition` | required | AWS Batch job definition |
| `--s3-bucket` | required | S3 bucket for task data |
| `--s3-prefix` | scaler-tasks | S3 prefix |
| `--max-concurrent-jobs` | 100 | Max concurrent Batch jobs |
| `--poll-interval` | 1.0 | Job status poll interval (s) |
| `--aws-region` | us-east-1 | AWS region |
