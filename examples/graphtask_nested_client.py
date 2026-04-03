"""Basic nested graph task example (recursive Fibonacci)."""

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def minus(a, b):
    return a - b


def fibonacci(client: Client, n: int):
    if n == 0:
        return 0
    if n == 1:
        return 1

    # fmt: off
    fib_graph = {
        "n": n,
        "one": 1,
        "two": 2,
        "n_minus_one": (minus, "n", "one"),
        "n_minus_two": (minus, "n", "two"),
    }
    result = client.get(fib_graph, keys=["n_minus_one", "n_minus_two"])
    n_minus_one = result.get("n_minus_one")
    n_minus_two = result.get("n_minus_two")
    a = client.submit(fibonacci, client, n_minus_one)
    b = client.submit(fibonacci, client, n_minus_two)
    return a.result() + b.result()


def main():
    cluster = SchedulerClusterCombo(n_workers=10)

    with Client(address=cluster.get_address()) as client:
        result = client.submit(fibonacci, client, 8).result()
        print(result)

    cluster.shutdown()


if __name__ == "__main__":
    main()
