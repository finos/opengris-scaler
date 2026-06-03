"""Basic Client.submit example with a local cluster."""

import math

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def main():
    cluster = SchedulerClusterCombo(n_workers=10)

    with Client(address=cluster.get_address()) as client:
        futures = [client.submit(math.sqrt, value) for value in range(100)]
        total = sum(future.result() for future in futures)
        print(total)

    cluster.shutdown()


if __name__ == "__main__":
    main()
