"""Basic client clear/disconnect example with a local cluster."""

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def main():
    cluster = SchedulerClusterCombo(n_workers=10)
    client = Client(address=cluster.get_address())
    client.clear()
    client.disconnect()

    cluster.shutdown()


if __name__ == "__main__":
    main()
