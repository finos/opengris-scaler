"""This example shows how to utilize graph task functionality provided by scaler."""

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def inc(i):
    return i + 1


def add(a, b):
    return a + b


def minus(a, b):
    return a - b


# A graph task is defined as a dict with str as the key type and val_t as the value type, where val_t is defined as
# follows:
# Union[Any, Tuple[Union[Callable, str], ...]
# Each value can be one of the following:
# - a basic data type (int, List, etc.),
# - a callable,
# - a tuple of the form (Callable, key_t val1, key_t val2, ...)
# that represents a function call.
graph = {
    "a": 2,
    "b": 2,
    "c": (inc, "a"),  # c = a + 1 = 2 + 1 = 3
    "d": (add, "a", "b"),  # d = a + b = 2 + 2 = 4
    "e": (minus, "d", "c"),  # e = d - c = 4 - 3 = 1
    "f": add,
}


def main():
    # For an explanation on how SchedulerClusterCombo and Client work, please see simple_client.py
    cluster = SchedulerClusterCombo(n_workers=1)

    with Client(address=cluster.get_address()) as client:
        # See graph's definition for more detail.
        # The result is a dictionary that contains the requested keys.
        # Each value provided in the graph will be evaluated and passed back.
        result = client.get(graph, keys=["a", "b", "c", "d", "e", "f"])
        print(result.get("e"))
        print(result)  # {'a': 2, 'b': 2, 'c': 3, 'd': 4, 'e': 1, 'f': <function add at 0x70af1e29b4c0>}

    cluster.shutdown()


if __name__ == "__main__":
    main()
