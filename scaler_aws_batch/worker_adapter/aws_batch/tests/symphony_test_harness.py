#!/usr/bin/env python3
"""
Test harness for sending sample tasks to Symphony worker adapter.

Usage:
    python tests/symphony_test_harness.py --scheduler tcp://127.0.0.1:2345 --symphony-url http://127.0.0.1:8080
"""

import argparse
import time
from typing import Any

from scaler import Client


def simple_task(x: int) -> int:
    """Simple task that doubles the input."""
    return x * 2


def sleep_task(seconds: int) -> int:
    """Task that sleeps for specified seconds."""
    time.sleep(seconds)
    return seconds


def heavy_computation(n: int) -> int:
    """Task that performs heavy computation."""
    result = 0
    for i in range(n):
        result += i ** 2
    return result


def nested_task(client: Client, x: int) -> int:
    """Task that submits another task (nested)."""
    future = client.submit(simple_task, x)
    return future.result() + 1


def error_task(should_fail: bool) -> str:
    """Task that raises an error if should_fail is True."""
    if should_fail:
        raise ValueError("Task intentionally failed")
    return "success"


def run_simple_test(client: Client):
    """Run a simple test with one task."""
    print("\n=== Running Simple Test ===")
    future = client.submit(simple_task, 5)
    result = future.result()
    print(f"Result: {result}")
    assert result == 10, f"Expected 10, got {result}"
    print("✓ Simple test passed")


def run_map_test(client: Client):
    """Run a test using map with multiple tasks."""
    print("\n=== Running Map Test ===")
    inputs = [1, 2, 3, 4, 5]
    results = client.map(simple_task, [(x,) for x in inputs])
    expected = [x * 2 for x in inputs]
    print(f"Results: {results}")
    assert results == expected, f"Expected {expected}, got {results}"
    print("✓ Map test passed")


def run_sleep_test(client: Client):
    """Run a test with sleep tasks."""
    print("\n=== Running Sleep Test ===")
    futures = [client.submit(sleep_task, i) for i in [1, 2, 1]]
    results = [f.result() for f in futures]
    print(f"Results: {results}")
    assert results == [1, 2, 1], f"Expected [1, 2, 1], got {results}"
    print("✓ Sleep test passed")


def run_heavy_computation_test(client: Client):
    """Run a test with heavy computation."""
    print("\n=== Running Heavy Computation Test ===")
    future = client.submit(heavy_computation, 10000)
    result = future.result()
    expected = sum(i ** 2 for i in range(10000))
    print(f"Result: {result}")
    assert result == expected, f"Expected {expected}, got {result}"
    print("✓ Heavy computation test passed")


def run_nested_test(client: Client):
    """Run a test with nested tasks."""
    print("\n=== Running Nested Task Test ===")
    future = client.submit(nested_task, client, 5)
    result = future.result()
    print(f"Result: {result}")
    assert result == 11, f"Expected 11, got {result}"
    print("✓ Nested task test passed")


def run_error_test(client: Client):
    """Run a test that expects an error."""
    print("\n=== Running Error Test ===")
    future = client.submit(error_task, True)
    try:
        future.result()
        print("✗ Error test failed - expected ValueError")
    except ValueError as e:
        print(f"Caught expected error: {e}")
        print("✓ Error test passed")


def run_batch_test(client: Client, num_tasks: int = 100):
    """Run a batch test with many tasks."""
    print(f"\n=== Running Batch Test ({num_tasks} tasks) ===")
    start_time = time.time()
    futures = [client.submit(simple_task, i) for i in range(num_tasks)]
    results = [f.result() for f in futures]
    elapsed = time.time() - start_time
    
    expected = [i * 2 for i in range(num_tasks)]
    assert results == expected, f"Results mismatch"
    print(f"Completed {num_tasks} tasks in {elapsed:.2f} seconds")
    print(f"Throughput: {num_tasks / elapsed:.2f} tasks/sec")
    print("✓ Batch test passed")


def main():
    parser = argparse.ArgumentParser(description="Symphony Worker Adapter Test Harness")
    parser.add_argument(
        "--scheduler",
        type=str,
        default="tcp://127.0.0.1:2345",
        help="Scheduler address (default: tcp://127.0.0.1:2345)",
    )
    parser.add_argument(
        "--test",
        type=str,
        choices=["simple", "map", "sleep", "heavy", "nested", "error", "batch", "all"],
        default="all",
        help="Test to run (default: all)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="Number of tasks for batch test (default: 100)",
    )
    
    args = parser.parse_args()
    
    print(f"Connecting to scheduler at {args.scheduler}")
    
    with Client(address=args.scheduler) as client:
        print("✓ Connected to scheduler")
        
        tests = {
            "simple": run_simple_test,
            "map": run_map_test,
            "sleep": run_sleep_test,
            "heavy": run_heavy_computation_test,
            "nested": run_nested_test,
            "error": run_error_test,
            "batch": lambda c: run_batch_test(c, args.batch_size),
        }
        
        if args.test == "all":
            for test_name, test_func in tests.items():
                try:
                    test_func(client)
                except Exception as e:
                    print(f"✗ {test_name} test failed: {e}")
        else:
            tests[args.test](client)
    
    print("\n=== All tests completed ===")


if __name__ == "__main__":
    main()
