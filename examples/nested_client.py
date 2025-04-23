"""This example shows how to created nested tasks. Please see graphtask_nested_client.py for more information."""

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


# Calculate fibonacci sequence with nested client.
# Each intermediate call in the recursive process is submitted to the client.
def fibonacci(client: Client, n: int):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        a = client.submit(fibonacci, client, n - 1)
        b = client.submit(fibonacci, client, n - 2)
        return a.result() + b.result()


def main():
    # For an explanation on how SchedulerClusterCombo and Client work, please see simple_client.py
    cluster = SchedulerClusterCombo(n_workers=1)

    with Client(address=cluster.get_address()) as client:
        result = client.submit(fibonacci, client, 8).result()
        print(result)  # 21

    cluster.shutdown()


if __name__ == "__main__":
    main()
