"""
This example demonstrates the most basic implementation to work with scaler
Scaler applications have three parts - scheduler, cluster, and client.
Scheduler is used to schedule works send from client to cluster.
Cluster, composed of 1 or more worker(s), are used to execute works.
Client is used to send tasks to scheduler.

This example shows a client sends 100 tasks, where each task represents the
execution of math.sqrt function and get back the results.
"""

import math

from scaler import Client
from scaler.cluster.combo import SchedulerClusterCombo


def main():
    # Instantiate a SchedulerClusterCombo which contains a scheduler and a cluster that contains n_workers workers. In
    # this case, there are 10 workers. There are more options to control the behavior of SchedulerClusterCombo, you can
    # check them out in other examples.
    cluster = SchedulerClusterCombo(n_workers=10)

    # Instantiate a Client that represents a client aforementioned. One may submit task using client.
    # Since client is sending task(s) to the scheduler, we need to know the address that the scheduler has. In this
    # case, we can get the address using cluster.get_address()
    with Client(address=cluster.get_address()) as client:
        # Submits 100 tasks
        futures = [
            # In each iteration of the loop, we submit one task to the scheduler. Each task represents the execution of
            # a function defined by you.
            # Note: Users are responsible to correctly provide the argument(s) of a function that the user wish to call.
            # Fail to do so results in exception.
            # This is to demonstrate client.submit(). A better way to implement this particular case is to use
            # client.map(). See `map_client.py` for more detail.
            client.submit(math.sqrt, i)
            for i in range(0, 100)
        ]

        # Each call to Client.submit returns a future. Users are expected to keep the future until the task has been
        # finished, or cancelled. The future returned by Client.submit is the only way to get results from corresponding
        # tasks. In this case, future.result() will return a float, but this can be any type should the user wish.
        result = sum(future.result() for future in futures)

        print(result)  # 661.46

    cluster.shutdown()


if __name__ == "__main__":
    main()
