"""
This example demonstrates various ways to submit tasks to a Scaler scheduler.
It shows how to use the Client to:
1. Submit a single task using .submit()
2. Submit multiple tasks using .map()
3. Submit tasks with multiple arguments using .map() and .starmap()
"""

import argparse
import math

from scaler import Client, SchedulerClusterCombo


def square(value: int):
    return value * value


def add(x: int, y: int):
    return x + y


def main():
    parser = argparse.ArgumentParser(description="Submit tasks to a Scaler scheduler.")
    parser.add_argument("url", nargs="?", help="The URL of the Scaler scheduler (e.g., tcp://127.0.0.1:2345)")
    args = parser.parse_args()

    cluster = None
    if args.url is None:

        print("No scheduler URL provided. Spinning up a local cluster...")
        cluster = SchedulerClusterCombo(n_workers=4)
        address = cluster.get_address()
    else:
        address = args.url

    try:
        print(f"Connecting to scheduler at {address}...")

        # Use the Client as a context manager to ensure proper cleanup
        with Client(address=address) as client:
            print("Submitting a single task using .submit()...")
            future = client.submit(square, 4)
            print(f"Result of square(4): {future.result()}")

            print("\nSubmitting multiple tasks using .map()...")
            # client.map() works like Python's built-in map()
            results = client.map(math.sqrt, range(1, 6))
            print(f"Results of sqrt(1..5): {list(results)}")

            print("\nSubmitting tasks with multiple arguments using .map()...")
            # You can pass multiple iterables to map() for functions with multiple arguments
            results_add = client.map(add, [1, 2, 3], [10, 20, 30])
            print(f"Results of add([1,2,3], [10,20,30]): {list(results_add)}")

            print("\nSubmitting tasks with multiple arguments using .starmap()...")
            # starmap() takes an iterable of argument tuples
            results_starmap = client.starmap(add, [(5, 5), (10, 10)])
            print(f"Results of starmap(add, [(5,5), (10,10)]): {list(results_starmap)}")
    finally:
        if cluster:
            cluster.shutdown()
    print("\nAll tasks completed successfully.")


if __name__ == "__main__":
    main()
