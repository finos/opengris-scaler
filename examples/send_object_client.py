"""
This example demonstrates how to use the Client.send_object() method.
This method is used to submit large objects to the cluster.
Users can then reuse this object without needing to retransmit it multiple times.
"""

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo

large_object = [1, 2, 3, 4, 5]


def query(object_reference, idx):
    return object_reference[idx]


def main():
    # For an explanation on how SchedulerClusterCombo and Client work, please see simple_client.py
    cluster = SchedulerClusterCombo(n_workers=1)
    client = Client(address=cluster.get_address())

    # Send the "large" to the cluster for reuse. Providing a name for the object is optional.
    # This method returns a reference to the object that we can use in place of the original object.
    large_object_ref = client.send_object(large_object, name="large_object")

    # Reuse through object reference
    # Note that this example is not very interesting, since query is essentially a cheap operation that should be done
    # in local end. We chose this operation since it demonstrates that operation and operator defined on the original
    # type (list) can be applied to the reference.
    fut1 = client.submit(query, large_object_ref, 0)
    fut2 = client.submit(query, large_object_ref, 1)

    # Get the result from the future.
    print(fut1.result())
    print(fut2.result())


if __name__ == "__main__":
    main()
