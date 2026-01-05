"""
AWS Batch Job Runner.

Runs inside AWS Batch containers to execute Scaler tasks.
Handles both inline (environment variable) and S3-based payloads,
with optional gzip compression.

Environment variables:
    SCALER_TASK_ID: Task ID
    SCALER_PAYLOAD: Base64-encoded payload (inline mode)
    SCALER_S3_BUCKET: S3 bucket
    SCALER_S3_KEY: S3 key for payload (S3 mode)
    SCALER_S3_PREFIX: S3 prefix for results
    SCALER_PAYLOAD_COMPRESSED: "1" if payload is gzip compressed
"""

import base64
import gzip
import logging
import os
import sys
import traceback

import boto3
import cloudpickle


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )


def get_payload() -> bytes:
    """Fetch task payload from inline env var or S3."""
    compressed = os.environ.get("SCALER_PAYLOAD_COMPRESSED", "0") == "1"
    
    # Try inline payload first
    inline_payload = os.environ.get("SCALER_PAYLOAD")
    if inline_payload:
        payload = base64.b64decode(inline_payload)
        if compressed:
            payload = gzip.decompress(payload)
        return payload
    
    # Fall back to S3
    s3_bucket = os.environ.get("SCALER_S3_BUCKET")
    s3_key = os.environ.get("SCALER_S3_KEY")
    
    if not s3_bucket or not s3_key:
        raise ValueError("No payload source: SCALER_PAYLOAD or SCALER_S3_KEY required")
    
    s3_client = boto3.client("s3")
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    payload = response["Body"].read()
    
    if compressed or s3_key.endswith(".gz"):
        payload = gzip.decompress(payload)
    
    # Cleanup input from S3
    try:
        s3_client.delete_object(Bucket=s3_bucket, Key=s3_key)
    except Exception as e:
        logging.warning(f"Failed to cleanup S3 input: {e}")
    
    return payload


def store_result(result: bytes, compressed: bool = False):
    """Store result to S3."""
    s3_bucket = os.environ.get("SCALER_S3_BUCKET")
    s3_prefix = os.environ.get("SCALER_S3_PREFIX", "scaler-tasks")
    job_id = os.environ.get("AWS_BATCH_JOB_ID", os.environ.get("SCALER_TASK_ID"))
    
    if not s3_bucket:
        raise ValueError("SCALER_S3_BUCKET required for result storage")
    
    # Compress if beneficial
    if len(result) > 4096:
        result = gzip.compress(result)
        compressed = True
    
    result_key = f"{s3_prefix}/results/{job_id}.pkl"
    if compressed:
        result_key += ".gz"
    
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=s3_bucket, Key=result_key, Body=result)
    
    logging.info(f"Result stored to s3://{s3_bucket}/{result_key}")


def main():
    setup_logging()
    
    task_id = os.environ.get("SCALER_TASK_ID", "unknown")
    logging.info(f"Starting task {task_id[:8]}...")
    
    try:
        # Get payload
        payload_bytes = get_payload()
        task_data = cloudpickle.loads(payload_bytes)
        
        logging.info(f"Task data loaded: {len(task_data.get('args', []))} arguments")
        
        # For now, task_data contains serialized references
        # The actual execution requires fetching from object storage
        # This is a placeholder - real implementation would connect to object storage
        
        # Execute task
        # In full implementation: fetch function and args from object storage, execute
        result = {"status": "executed", "task_id": task_id}
        
        # Serialize and store result
        result_bytes = cloudpickle.dumps(result)
        store_result(result_bytes)
        
        logging.info(f"Task {task_id[:8]} completed successfully")
        
    except Exception as e:
        logging.error(f"Task {task_id[:8]} failed: {e}")
        traceback.print_exc()
        
        # Store error result
        try:
            error_result = {
                "error": str(e),
                "traceback": traceback.format_exc(),
            }
            store_result(cloudpickle.dumps(error_result))
        except Exception as store_error:
            logging.error(f"Failed to store error result: {store_error}")
        
        sys.exit(1)


if __name__ == "__main__":
    main()
