"""Basic nested submit example (recursive Fibonacci)."""

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def fibonacci(client: Client, n: int):
    if n == 0:
        return 0
    if n == 1:
        return 1

    a = client.submit(fibonacci, client, n - 1)
    b = client.submit(fibonacci, client, n - 2)
    return a.result() + b.result()


def main():
    cluster = SchedulerClusterCombo(n_workers=1)

    with Client(address=cluster.get_address()) as client:
        result = client.submit(fibonacci, client, 8).result()
        print(result)

    cluster.shutdown()


if __name__ == "__main__":
    main()
