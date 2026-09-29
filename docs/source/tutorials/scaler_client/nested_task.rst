Nested Task (Submit A Parallel Task Within A Parallel Task)
===========================================================

Nested tasks let tasks submit additional tasks without building a full graph up front.

.. literalinclude:: ../../../../examples/nested_client.py
   :language: python

What the example does:

* Submits ``fibonacci`` as a remote task.
* Inside each task, recursively submits child tasks with :py:func:`~Client.submit()`.
* Waits on child futures and combines their results.

This pattern is useful to demonstrate nested execution, but recursion creates many small tasks and can be expensive.

Nested client addresses
-----------------------

A nested ``Client`` given neither address takes both from the worker context.

- ``address``: the scheduler address its worker is connected to.
- ``object_storage_address``: the address its worker reaches object storage on.

Pass ``address`` to reach another scheduler. Object storage is then the address that scheduler
advertises. Pass ``object_storage_address`` to set it directly.

This matters where the addresses a worker uses differ from the ones outside the cluster, for example
behind NAT or a load balancer. See :ref:`object-storage-addresses`.

.. code:: python

    from scaler import Client


    def nested_task():
        # Takes the scheduler and object storage addresses from the worker
        with Client() as client:
            result = client.submit(lambda x: x * 2, 5).result()
        return result


    client = Client(address="tcp://127.0.0.1:2345")
    future = client.submit(nested_task)
    print(future.result())  # 10
