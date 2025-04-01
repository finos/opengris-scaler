"""This example shows how to clear resources owned by a client and how to disconnect a client from the scheduler."""

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def main():
    cluster = SchedulerClusterCombo(n_workers=10)
    client = Client(address=cluster.get_address())
    # Client.clear() will clear all computation resources owned by the client. All unfinished tasks will be cancelled,
    # and all object reference will be invalidated. The client can submit tasks as it wishes.
    client.clear()

    # Once disconnect is called, this client is invalidated, and no tasks can be installed on this client.
    # Should the user wish to initiate more tasks, they should instantiate another Client. The scheduler's running
    # state will not be affected by this method.
    client.disconnect()

    # The user may also choose to shutdown the scheduler while disconnecting client from the scheduler. Such a request
    # is not guaranteed to succeed, as the scheduler can only be closed when it is not running under "protected" mode.
    # client.shutdown()


if __name__ == "__main__":
    main()
