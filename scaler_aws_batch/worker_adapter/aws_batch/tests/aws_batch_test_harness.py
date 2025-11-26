#!/usr/bin/env python3
"""
Test harness for AWS Batch worker adapter.

Usage:
    python tests/aws_batch_test_harness.py --scheduler tcp://127.0.0.1:2345
"""

import argparse
import time
from scaler import Client


def simple_task(x: int) -> int:
    """Simple task that doubles the input."""
    return x * 2


def run_simple_test(client: Client):
    """Run a simple test with one task."""
    print("\n=== Running AWS Batch Simple Test ===")
    future = client.submit(simple_task, 5)
    result = future.result()
    print(f"Result: {result}")
    assert result == 10, f"Expected 10, got {result}"
    print("✓ AWS Batch simple test passed")


def main():
    parser = argparse.ArgumentParser(description="AWS Batch Worker Adapter Test Harness")
    parser.add_argument(
        "--scheduler",
        type=str,
        default="tcp://127.0.0.1:2345",
        help="Scheduler address (default: tcp://127.0.0.1:2345)",
    )
    
    args = parser.parse_args()
    
    print(f"Connecting to scheduler at {args.scheduler}")
    
    with Client(address=args.scheduler) as client:
        print("✓ Connected to scheduler")
        run_simple_test(client)
    
    print("\n=== AWS Batch test completed ===")


if __name__ == "__main__":
    main()