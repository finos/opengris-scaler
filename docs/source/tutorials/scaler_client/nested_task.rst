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

Nested client scheduler address
-------------------------------

When creating a nested ``Client`` from inside a task, the ``address`` argument is optional.
If omitted, the client can detect and use the scheduler address from the worker context.
This is useful in environments where external and worker-visible scheduler addresses differ (for example behind NAT/firewalls).

.. code:: python

    from scaler import Client


    def nested_task():
        # Automatically uses worker-visible scheduler address
        with Client() as client:
            result = client.submit(lambda x: x * 2, 5).result()
        return result


    client = Client(address="tcp://127.0.0.1:2345")
    future = client.submit(nested_task)
    print(future.result())  # 10
