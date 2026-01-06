#!/usr/bin/env python3
"""
AWS Batch Worker Adapter Test Harness.

Tests the AWS Batch worker adapter with simple tasks, map operations,
and compute-intensive tasks.

Usage:
    python tests/aws_batch_test_harness.py --scheduler tcp://127.0.0.1:2345 --test all
    python tests/aws_batch_test_harness.py --scheduler tcp://127.0.0.1:2345 --test simple
"""

import argparse
import sys
import time

from scaler import Client

# EC2 cold start can take 2-3 minutes, so use a generous timeout
DEFAULT_TIMEOUT = 300  # 5 minutes


def simple_task(x: int) -> int:
    """Simple task that doubles the input."""
    return x * 2


def square(x: int) -> int:
    """Square a number."""
    return x * x


def compute_task(n: int) -> float:
    """Compute-intensive task: sum of squares."""
    total = 0.0
    for i in range(n):
        total += i * i * 0.01
    return total


def run_simple_test(client: Client, timeout: int) -> bool:
    """Run a simple test with one task."""
    print("\n--- Test: Simple Task ---")
    try:
        future = client.submit(simple_task, 21)
        result = future.result(timeout=timeout)
        print(f"  Result: {result}")
        if result == 42:
            print("  PASSED")
            return True
        else:
            print(f"  FAILED: Expected 42, got {result}")
            return False
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def run_map_test(client: Client, timeout: int) -> bool:
    """Run a map test with multiple tasks."""
    print("\n--- Test: Map Tasks ---")
    try:
        inputs = list(range(5))
        # client.map() returns results directly, not futures
        results = client.map(simple_task, [(x,) for x in inputs])
        print(f"  Results: {results}")
        expected = [0, 2, 4, 6, 8]
        if results == expected:
            print("  PASSED")
            return True
        else:
            print(f"  FAILED: Expected {expected}, got {results}")
            return False
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def run_compute_test(client: Client, timeout: int) -> bool:
    """Run a compute-intensive test."""
    print("\n--- Test: Compute Task ---")
    try:
        future = client.submit(compute_task, 1000)
        result = future.result(timeout=timeout)
        print(f"  Result: {result:.2f}")
        # Expected: sum of i*i*0.01 for i in range(1000) = 332833500 * 0.01 = 3328335.0
        if 3000000 < result < 4000000:
            print("  PASSED")
            return True
        else:
            print(f"  FAILED: Result out of expected range")
            return False
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="AWS Batch Worker Adapter Test Harness")
    parser.add_argument(
        "--scheduler",
        type=str,
        default="tcp://127.0.0.1:2345",
        help="Scheduler address (default: tcp://127.0.0.1:2345)",
    )
    parser.add_argument(
        "--test",
        type=str,
        default="all",
        choices=["all", "simple", "map", "compute"],
        help="Test to run (default: all)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Timeout in seconds for each task (default: {DEFAULT_TIMEOUT})",
    )

    args = parser.parse_args()

    print("=" * 50)
    print("AWS Batch Worker Adapter Test Harness")
    print("=" * 50)
    print(f"Scheduler: {args.scheduler}")
    print(f"Timeout: {args.timeout}s")

    try:
        with Client(address=args.scheduler) as client:
            print("Connected to scheduler")

            tests = {
                "simple": run_simple_test,
                "map": run_map_test,
                "compute": run_compute_test,
            }

            if args.test == "all":
                tests_to_run = list(tests.keys())
            else:
                tests_to_run = [args.test]

            passed = 0
            total = len(tests_to_run)

            for test_name in tests_to_run:
                if tests[test_name](client, args.timeout):
                    passed += 1

            print("\n" + "=" * 50)
            print(f"Results: {passed}/{total} passed")

            if passed == total:
                print("All tests passed!")
                sys.exit(0)
            else:
                print("Some tests failed.")
                sys.exit(1)

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
