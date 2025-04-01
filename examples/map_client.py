"""
This example shows how to use the Client.map() method.
Client.map() allows the user to invoke a callable many times with different values.
For more information on the map operation, refer to
https://en.wikipedia.org/wiki/Map_(higher-order_function)
"""

import math

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def main():
    # For an explanation on how SchedulerClusterCombo and Client work, please see simple_client.py
    cluster = SchedulerClusterCombo(n_workers=10)
    client = Client(address=cluster.get_address())

    # map each integer in [0, 100) through math.sqrt()
    # the first parameter is the function to call, and the second is a list of argument tuples
    # (x,) denotes a tuple of length one
    results = client.map(math.sqrt, [(x,) for x in range(100)])

    # Collect the results and sums them
    result = sum(results)

    print(result)


if __name__ == "__main__":
    main()
