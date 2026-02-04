"""
This example shows how to use the Client.map() and Client.starmap() methods.
Client.map() allows the user to invoke a callable many times with different values.
Client.starmap() is similar but unpacks argument tuples, like itertools.starmap().
For more information on the map operation, refer to
https://en.wikipedia.org/wiki/Map_(higher-order_function)
"""

import math

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def main():
    # For an explanation on how SchedulerClusterCombo and Client work, please see simple_client.py
    cluster = SchedulerClusterCombo(n_workers=10)

    with Client(address=cluster.get_address()) as client:
        # map() works like Python's built-in map(): pass a function and one or more iterables
        # Each element from the iterable becomes an argument to the function
        results = client.map(math.sqrt, range(100))

        # Collect the results and sum them
        result = sum(results)
        print(f"Sum of square roots: {result}")

        # For functions with multiple arguments, pass multiple iterables
        def add(x, y):
            return x + y

        results = client.map(add, [1, 2, 3], [10, 20, 30])
        print(f"Pairwise sums: {results}")  # [11, 22, 33]

        # starmap() unpacks argument tuples, like itertools.starmap()
        results = client.starmap(add, [(1, 10), (2, 20), (3, 30)])
        print(f"Starmap sums: {results}")  # [11, 22, 33]

    cluster.shutdown()


if __name__ == "__main__":
    main()
