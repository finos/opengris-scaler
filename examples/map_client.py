"""Basic Client.map and Client.starmap example with a local cluster."""

import math

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def add(x, y):
    return x + y


def main():
    cluster = SchedulerClusterCombo(n_workers=10)

    with Client(address=cluster.get_address()) as client:
        results = client.map(math.sqrt, range(100))
        print(sum(results))

        results = client.map(add, [1, 2, 3], [10, 20, 30])
        print(results)

        results = client.starmap(add, [(1, 10), (2, 20), (3, 30)])
        print(results)

    cluster.shutdown()


if __name__ == "__main__":
    main()
