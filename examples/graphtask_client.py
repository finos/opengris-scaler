"""Basic graph task example with Client.get."""

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def inc(i):
    return i + 1


def add(a, b):
    return a + b


def minus(a, b):
    return a - b


# fmt: off
graph = {
    "a": 2,
    "b": 2,
    "c": (inc, "a"),
    "d": (add, "a", "b"),
    "e": (minus, "d", "c"),
    "f": add,
}


def main():
    cluster = SchedulerClusterCombo(n_workers=1)

    with Client(address=cluster.get_address()) as client:
        result = client.get(graph, keys=["a", "b", "c", "d", "e", "f"])
        print(result.get("e"))
        print(result)

    cluster.shutdown()


if __name__ == "__main__":
    main()
