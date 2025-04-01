"""This example shows how to build graph dynamically in the remote side"""

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def minus(a, b):
    return a - b


def fibonacci(clnt: Client, n: int):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        # Dynamically building graph in the worker side is okay.
        # BE WARNED! You are not suppose to use it like that. This is to demonstrate the ability instead of intention
        # of what graph can do. This should rarely be done. Redesign if you find yourself in this position. With the
        # ability to dynamically build a graph, one can even concatenate the source graph to its child (as long as they
        # evaluate to a value).
        fib_graph = {"n": n, "one": 1, "two": 2, "n_minus_one": (minus, "n", "one"), "n_minus_two": (minus, "n", "two")}
        res = clnt.get(fib_graph, keys=["n_minus_one", "n_minus_two"])
        n_minus_one = res.get("n_minus_one")
        n_minus_two = res.get("n_minus_two")
        a = clnt.submit(fibonacci, clnt, n_minus_one)
        b = clnt.submit(fibonacci, clnt, n_minus_two)
        return a.result() + b.result()


def main():
    # For an explanation on how SchedulerClusterCombo and Client work, please see simple_client.py
    cluster = SchedulerClusterCombo(n_workers=10)
    client = Client(address=cluster.get_address())
    result = client.submit(fibonacci, client, 8).result()
    print(result)  # 21


if __name__ == "__main__":
    main()
