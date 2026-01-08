#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$SCRIPT_DIR/.scaler_aws_hpc.env"

# Check if env file exists
if [ ! -f "$ENV_FILE" ]; then
    echo "Error: $ENV_FILE not found"
    echo "Run provisioner first: python src/scaler/utility/worker_adapter/aws_hpc/provisioner.py provision ..."
    exit 1
fi

# Load environment variables
source "$ENV_FILE"

echo "Starting AWS Batch integration tests..."
echo "  Region: $SCALER_AWS_REGION"
echo "  S3 Bucket: $SCALER_S3_BUCKET"
echo "  Job Queue: $SCALER_JOB_QUEUE"
echo "  Job Definition: $SCALER_JOB_DEFINITION"
echo ""

# Start scheduler in background
echo "Starting scheduler..."
python -c "
import sys
sys.argv = ['scheduler', 'tcp://0.0.0.0:2345']
from scaler.entry_points.scheduler import main
main()
" &
SCHEDULER_PID=$!

sleep 2

# Start AWS Batch worker in background
echo "Starting AWS Batch worker..."
python -m scaler.entry_points.worker_adapter_aws_hpc \
    --scheduler-address tcp://127.0.0.1:2345 \
    --job-queue "$SCALER_JOB_QUEUE" \
    --job-definition "$SCALER_JOB_DEFINITION" \
    --s3-bucket "$SCALER_S3_BUCKET" \
    --aws-region "$SCALER_AWS_REGION" \
    --max-concurrent-jobs 100 \
    --log-level INFO &
WORKER_PID=$!

sleep 2

# Cleanup function
cleanup() {
    echo ""
    echo "Stopping background processes..."
    kill $SCHEDULER_PID $WORKER_PID 2>/dev/null || true
    wait $SCHEDULER_PID $WORKER_PID 2>/dev/null || true
    echo "Cleanup complete"
}

# Register cleanup on exit
trap cleanup EXIT INT TERM

# Run tests
echo "Running tests..."
echo ""
python "$SCRIPT_DIR/aws_hpc_test_harness.py" \
    --scheduler tcp://127.0.0.1:2345 \
    --test all

echo ""
echo "Tests complete!"
