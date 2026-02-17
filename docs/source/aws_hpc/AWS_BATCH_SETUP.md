# AWS Batch Worker Adapter Setup Guide

This guide walks you through setting up and testing the AWS Batch worker adapter for Scaler.

## Architecture

```
┌─────────┐     ┌───────────┐     ┌─────────────────┐     ┌───────────────────┐     ┌───────────┐
│  Client │────▶│ Scheduler │────▶│ AWSBatchWorker  │────▶│ AWSHPCTaskManager │────▶│ AWS Batch │
└─────────┘     └───────────┘     └─────────────────┘     └───────────────────┘     └───────────┘
                                          │                        │                      │
                                          ▼                        ▼                      ▼
                                  ┌────────────────┐         ┌──────────┐          ┌──────────┐
                                  │HeartbeatManager│         │ S3 Bucket│◀─────────│ Batch Job│
                                  └────────────────┘         └──────────┘          └──────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **Client** | Submits tasks to the scheduler using the Scaler API |
| **Scheduler** | Distributes tasks to available workers via ZMQ streaming |
| **AWSBatchWorker** | Process that connects to scheduler, routes messages to TaskManager |
| **AWSHPCTaskManager** | Handles task queuing, priority, concurrency control, and AWS Batch job submission |
| **HeartbeatManager** | Sends periodic heartbeats to scheduler with worker status |
| **S3 Bucket** | Stores task payloads (for large tasks) and job results |
| **AWS Batch** | Executes tasks as containerized jobs on EC2 compute environment |

## Payload Handling

Task payloads are passed to AWS Batch jobs via job parameters (30KiB limit).
For larger payloads, data is uploaded to S3.

| Raw Payload Size | After Compression | Method |
|------------------|-------------------|--------|
| ≤ 4KB | No compression | Job parameters (inline) |
| 4KB - 100KB | ~28KB (typical 3-5x ratio) | Job parameters (inline) |
| > 100KB | > 28KB | S3 upload |

Payloads > 4KB are automatically compressed with gzip before submission.

## Prerequisites

- Docker (for devcontainer)
- AWS Account with permissions for:
  - S3 (create bucket, read/write objects)
  - IAM (create roles and policies)
  - AWS Batch (create compute environments, job queues, job definitions)
  - ECR (create repository, push images)
- AWS CLI configured with credentials on host (for provisioning only)

## Quick Overview

| Step | Where | What |
|------|-------|------|
| 1. Provision | Host | Build image and create AWS resources |
| 2-6. Everything else | Container | Install, run, test |
| Cleanup | Host | Delete AWS resources |

---

## Step 1: Provision AWS Resources (Host Only)

> **⚠️ Note:** This provisioner creates resources for quick testing and development purposes only. 
> For production deployments, use your organization's infrastructure-as-code tools (CloudFormation, CDK, Terraform) 
> with proper security configurations, VPC settings, and resource tagging.

**Prerequisites for this step:**
1. Valid AWS credentials with sufficient permissions (S3, IAM, ECR, Batch, EC2)
2. Docker daemon is running

This step builds the Docker image, pushes it to ECR, and creates all AWS Batch resources:

```bash
# Create a temporary venv for provisioning (if not already created)
python3 -m venv .venv-provision

# Install boto3 in the provisioning venv
.venv-provision/bin/pip install boto3

# Provision all resources (builds image automatically)
.venv-provision/bin/python src/scaler/utility/worker_adapter/aws_hpc/provisioner.py provision \
    --region us-east-1 \
    --prefix scaler-batch \
    --vcpus 1 \
    --memory 2048
```

This creates:
- ECR repository with Docker image
- S3 bucket for task data (with 1-day lifecycle policy)
- IAM roles for Batch jobs and EC2 instances
- EC2 compute environment
- Job queue and job definition
- `tests/worker_adapter/aws_hpc/.scaler_aws_hpc.env` file with configuration
- `.scaler_aws_batch_config.json` file with full resource details (used for cleanup)

**Memory Configuration:**
Memory is rounded to the nearest multiple of 2048MB and 90% is allocated to the container.
For example, `--memory 4000` → 4096MB total → 3686MB effective.

### Using Existing Infrastructure

If you already have AWS Batch resources (created via CloudFormation, CDK, Terraform, etc.), skip the provisioner and create `tests/worker_adapter/aws_hpc/.scaler_aws_hpc.env` manually:

```bash
mkdir -p tests/worker_adapter/aws_hpc
cat > tests/worker_adapter/aws_hpc/.scaler_aws_hpc.env << 'EOF'
export SCALER_AWS_REGION="us-east-1"
export SCALER_S3_BUCKET="your-existing-bucket"
export SCALER_JOB_QUEUE="your-existing-queue"
export SCALER_JOB_DEFINITION="your-existing-job-def"
EOF
```

Then continue from Step 2. The `.scaler_aws_batch_config.json` file is only needed for the provisioner's `cleanup` command.

---

## Step 2: Start Development Container (All Remaining Steps Inside Container)

From here on, **everything runs inside the container**.

```bash
# Build the development container (if not already built)
docker build -t scaler-dev -f .devcontainer/Dockerfile .
```

### Option A: Static AWS credentials (~/.aws/credentials file)

If you have static credentials in `~/.aws/credentials`:

```bash
docker run -it --rm \
    -v ~/.aws:/root/.aws:ro \
    -v $(pwd):/workspace -w /workspace scaler-dev bash
```

### Option B: Assumed role credentials (isengardcli, SSO, etc.)

If you use `credential_process`, SSO, or assumed roles (like isengardcli), export credentials as environment variables first:

```bash
# On host: Export credentials from AWS CLI
eval $(aws configure export-credentials --format env)

# Start container with credentials passed as env vars
docker run -it --rm \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e AWS_SESSION_TOKEN \
    -e AWS_DEFAULT_REGION=us-east-1 \
    -v $(pwd):/workspace -w /workspace scaler-dev bash
```

**Note:** Session credentials expire (typically 1 hour). If you get credential errors, exit the container and repeat the export/docker run steps.

---

## Step 3: Install Dependencies (Inside Container)

Inside the container:

```bash
cd /workspace

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install scaler in development mode
python -m pip install -e ".[dev]"

# Install AWS dependencies
python -m pip install boto3
```

---

## Step 4: Start Scheduler and Worker (Inside Container)

All commands in the **same terminal** inside the container:

```bash
source .venv/bin/activate

# Start scheduler in background (binds to all interfaces for host access)
python -c "
import sys
sys.argv = ['scheduler', 'tcp://0.0.0.0:2345']
from scaler.entry_points.scheduler import main
main()
" &

# Wait for scheduler to start
sleep 2

# Load AWS config from provisioning
source tests/worker_adapter/aws_hpc/.scaler_aws_hpc.env

# Start AWS Batch worker in background (default job timeout: 60 minutes)
python -m scaler.entry_points.worker_adapter_aws_hpc \
    --scheduler-address tcp://127.0.0.1:2345 \
    --job-queue $SCALER_JOB_QUEUE \
    --job-definition $SCALER_JOB_DEFINITION \
    --s3-bucket $SCALER_S3_BUCKET \
    --aws-region $SCALER_AWS_REGION \
    --max-concurrent-jobs 100 \
    --log-level INFO &

# To override job timeout (e.g., 10 minutes):
# python -m scaler.entry_points.worker_adapter_aws_hpc \
#     ... \
#     --job-timeout 10 &
```

You should see log output indicating both are running.

---

## Step 5: Run Tests

Same terminal inside the container:

```bash
python tests/worker_adapter/aws_hpc/aws_hpc_test_harness.py \
    --scheduler tcp://127.0.0.1:2345 \
    --test all
```

Expected output:
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
  Submitting: compute_task(1000) [sum of i*i*0.01 for i in range(1000)]
  Result: 3328335.00
  PASSED

==================================================
Results: 4/4 passed
```

---

## Step 6: Stop Background Processes

Inside the container:

```bash
# List background jobs
jobs

# Kill all background jobs
kill %1 %2

# Or kill by name
pkill -f "scaler.entry_points"
```

---

## Step 7: Use in Your Code

```python
from scaler import Client

# Connect to scheduler
with Client(address="tcp://127.0.0.1:2345") as client:
    # Submit a single task
    future = client.submit(my_function, arg1, arg2)
    result = future.result()

    # Submit multiple tasks
    results = client.map(my_function, [(arg1,), (arg2,), (arg3,)])
```

---

## Cleanup (Host Only)

When done, clean up AWS resources (run on host where you have AWS credentials):

```bash
# On host machine (where you have AWS credentials)
.venv-provision/bin/python src/scaler/utility/worker_adapter/aws_hpc/provisioner.py cleanup \
    --region us-east-1 \
    --prefix scaler-batch
```

---

## Troubleshooting

### Container hangs on startup or AWS operations

This usually means boto3 is trying to resolve credentials but can't. Common causes:

1. **Using `~/.aws` mount with `credential_process`**: The credential helper (like isengardcli) isn't available inside the container. Use Option B (environment variables) instead.

2. **Expired session credentials**: If using assumed roles, credentials expire. Re-export them on the host and restart the container.

3. **Missing credentials entirely**: Verify credentials are set inside container:
   ```bash
   env | grep AWS
   ```

### "No scaler job queue found"

Run the provisioner (Step 1).

### "Failed to connect to scheduler"

1. Check scheduler is running: `ps aux | grep scheduler`
2. Check port mapping: container started with `-p 2345:2345`
3. Check address is correct

### "AWS Batch job failed"

1. Check CloudWatch Logs for the job (logs retained for 30 days)
2. Verify IAM role has S3 permissions
3. Check S3 bucket exists and is accessible

### "Timeout waiting for result"

1. Check AWS Batch console for job status
2. Increase `--max-concurrent-jobs` if jobs are queued
3. Check compute environment has capacity

### Container image issues

Make sure your job definition uses an image with:
- Python 3.12 (must match client Python version for cloudpickle compatibility)
- `cloudpickle` and `boto3` installed

---

## Configuration Reference

### Provisioner Options

| Option | Default | Description |
|--------|---------|-------------|
| `--region` | us-east-1 | AWS region |
| `--prefix` | scaler-batch | Resource name prefix |
| `--image` | (auto-build) | Container image URI (if omitted, builds and pushes to ECR) |
| `--vcpus` | 1 | vCPUs per job |
| `--memory` | 2048 | Memory per job (MB, uses 90% of nearest 2048MB multiple) |
| `--max-vcpus` | 256 | Max vCPUs for compute env |
| `--instance-types` | default_x86_64 | EC2 instance types (comma-separated) |
| `--job-timeout` | 60 | Job timeout in minutes (default: 1 hour, overridden by worker at runtime) |

### Worker Options

| Option | Default | Description |
|--------|---------|-------------|
| `--scheduler-address` | required | Scheduler address |
| `--job-queue` | required | AWS Batch job queue |
| `--job-definition` | required | AWS Batch job definition |
| `--s3-bucket` | required | S3 bucket for task data |
| `--s3-prefix` | scaler-tasks | S3 prefix |
| `--max-concurrent-jobs` | 100 | Max concurrent Batch jobs |
| `--job-timeout` | 60 | Job timeout in minutes (default: 1 hour, overrides job definition timeout) |
| `--aws-region` | us-east-1 | AWS region |
