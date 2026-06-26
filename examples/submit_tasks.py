"""
This example demonstrates various ways to submit tasks to a Scaler scheduler.
It shows how to use the Client to:
1. Submit a single task using .submit()
2. Submit multiple tasks using .map()
3. Submit tasks with multiple arguments using .map() and .starmap()
"""

import argparse
import math
import time

from scaler import Client, SchedulerClusterCombo

DEFAULT_TASK_COUNT = 500
DEFAULT_WORKLOAD = 200_000


def square(value: int) -> int:
    return value * value


def add(x: int, y: int) -> int:
    return x + y


def cpu_bound(n: int) -> int:
    """Count primes up to n using trial division — pure CPU work."""
    count = 0
    for candidate in range(2, n):
        if all(candidate % divisor != 0 for divisor in range(2, int(candidate**0.5) + 1)):
            count += 1
    return count


def mixed_work(index: int, workload: int) -> dict:
    """CPU work followed by a short sleep to simulate mixed I/O+CPU load."""
    start = time.monotonic()
    prime_count = cpu_bound(workload)
    time.sleep(0.05)
    elapsed = time.monotonic() - start
    return {"index": index, "primes": prime_count, "elapsed": elapsed}


def main():
    parser = argparse.ArgumentParser(description="Submit tasks to a Scaler scheduler.")
    parser.add_argument("url", nargs="?", help="The URL of the Scaler scheduler (e.g., tcp://127.0.0.1:2345)")
    parser.add_argument("--count", type=int, default=DEFAULT_TASK_COUNT, help="Number of heavy tasks to submit")
    parser.add_argument(
        "--workload",
        type=int,
        default=DEFAULT_WORKLOAD,
        help="Upper bound for prime counting per task (controls CPU time)",
    )
    parser.add_argument("--workers", type=int, default=8, help="Number of local workers (ignored if url is provided)")
    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count must be >= 1")

    cluster = None
    if args.url is None:
        print("No scheduler URL provided. Spinning up a local cluster...")
        cluster = SchedulerClusterCombo(n_workers=args.workers)
        address = cluster.get_address()
    else:
        address = args.url

    try:
        print(f"Connecting to scheduler at {address}...")

        with Client(address=address) as client:
            print("Submitting a single task using .submit()...")
            future = client.submit(square, 4)
            print(f"Result of square(4): {future.result()}")

            print("\nSubmitting multiple tasks using .map()...")
            results = client.map(math.sqrt, range(1, 6))
            print(f"Results of sqrt(1..5): {list(results)}")

            print("\nSubmitting tasks with multiple arguments using .map()...")
            results_add = client.map(add, [1, 2, 3], [10, 20, 30])
            print(f"Results of add([1,2,3], [10,20,30]): {list(results_add)}")

            print("\nSubmitting tasks with multiple arguments using .starmap()...")
            results_starmap = client.starmap(add, [(5, 5), (10, 10)])
            print(f"Results of starmap(add, [(5,5), (10,10)]): {list(results_starmap)}")

            print(f"\nSubmitting {args.count} heavy CPU tasks (prime count up to {args.workload}) via .map()...")
            batch_start = time.monotonic()
            heavy_results = list(client.map(cpu_bound, [args.workload] * args.count))
            batch_elapsed = time.monotonic() - batch_start
            print(
                f"Completed {len(heavy_results)} heavy tasks in {batch_elapsed:.1f}s "
                f"({batch_elapsed / len(heavy_results) * 1000:.1f} ms/task avg)"
            )

            print(f"\nSubmitting {args.count} mixed I/O+CPU tasks via .starmap()...")
            batch_start = time.monotonic()
            mixed_results = list(client.starmap(mixed_work, [(i, args.workload) for i in range(args.count)]))
            batch_elapsed = time.monotonic() - batch_start
            slowest = max(r["elapsed"] for r in mixed_results)
            print(
                f"Completed {len(mixed_results)} mixed tasks in {batch_elapsed:.1f}s "
                f"(slowest task: {slowest * 1000:.1f} ms)"
            )
    finally:
        if cluster:
            cluster.shutdown()

    print("\nAll tasks completed successfully.")


if __name__ == "__main__":
    main()
