Quickstart
==========

Quick Installation
------------------

``opengris-scaler`` is published on PyPI; create a virtual environment and install with
``pip install opengris-scaler[all]`` to get started quickly, and see
:ref:`introduction_installation` for other install choices and optional dependencies.

Why use it
----------

Use Scaler when you want a very simple way to access and orchestrate compute resources across
multiple environments from one programming model.

Scaler is a good fit when you want to:

* start fast on local resources (single machine or on-prem cluster), and
* expand to cloud resources as workloads grow,
* while keeping the same client-side task submission workflow.

This is especially useful for teams that want easy access to different cloud compute backends
without rewriting application logic for each infrastructure provider.

Client.submit
-------------

There is another way of to submit task to the scheduler: :py:func:`~Client.submit()`, which is used to submit a single function and arguments. The results will be lazily retrieved on the first call to ``result()``.

.. literalinclude:: ../../../examples/simple_client.py
   :language: python


Things to Avoid
---------------

please note that the :py:func:`~Client.submit()` method is used to submit a single task. If you wish to submit multiple tasks using the same function but with many sets of arguments, use :py:func:`~Client.map()` instead to avoid unnecessary serialization overhead. The following is an example `what not to do`.

.. testcode:: python

    import functools
    import random

    from scaler import Client, SchedulerClusterCombo

    def lookup(heavy_map: bytes, index: int):
        return index * 1


    def main():
        address = "tcp://127.0.0.1:2345"

        cluster = SchedulerClusterCombo(address=address, n_workers=3)

        # a heavy function that is expensive to serialize
        big_func = functools.partial(lookup, b"1" * 5_000_000_000)

        arguments = [random.randint(0, 100) for _ in range(100)]

        with Client(address=address) as client:
            # we incur serialization overhead for every call to client.submit -- use client.map instead
            futures = [client.submit(big_func, i) for i in arguments]
            print([fut.result() for fut in futures])

        cluster.shutdown()


    if __name__ == "__main__":
        main()


This will be extremely slow, because it will serialize the argument function ``big_func()`` each time :py:func:`~Client.submit()` is called.

Functions may also be 'heavy' if they accept large objects as arguments. In this case, consider using :py:func:`~Client.send_object()` to send the object to the scheduler, and then later use :py:func:`~Client.submit()` to submit the function.

Spinning up Scheduler and Cluster Separately
--------------------------------------------

The object storage server, scheduler and workers can be spun up independently through the CLI.
Here we use localhost addresses for demonstration, however the scheduler and workers can be started on different machines.


.. code:: bash

    scaler_object_storage_server tcp://127.0.0.1:8517


.. code:: bash

    scaler_scheduler tcp://127.0.0.1:8516 -osa tcp://127.0.0.1:8517


.. code:: console

    [INFO]2025-06-06 13:30:05+0200: logging to ('/dev/stdout',)
    [INFO]2025-06-06 13:30:05+0200: use event loop: builtin
    [INFO]2025-06-06 13:30:05+0200: Scheduler: listen to scheduler address tcp://127.0.0.1:8516
    [INFO]2025-06-06 13:30:05+0200: Scheduler: connect to object storage server tcp://127.0.0.1:8517
    [INFO]2025-06-06 13:30:05+0200: Scheduler: listen to scheduler monitor address tcp://127.0.0.1:8518


.. code:: bash

    scaler_worker_manager native --mode fixed --max-task-concurrency 10 tcp://127.0.0.1:8516


From here, connect the Python Client and begin submitting tasks:

.. code:: python

    from scaler import Client


    def square(value):
        return value * value


    with Client(address="tcp://127.0.0.1:8516") as client:
        results = client.map(square, range(0, 100))

    print(results)
