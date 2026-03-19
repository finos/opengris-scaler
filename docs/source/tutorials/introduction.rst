Introduction
============
Architecture
------------

Below is a diagram of the relationship between multiple Clients, the Scheduler,
Worker Managers, and Workers.

.. code-block:: text

                                       +-------------------+                      +------------------+      +----------+
                                       |                   |                      |                  | ---> | Worker 1 |
    +----------+                       |                   |                      |                  |      +----------+
    | Client A | --------------------> |                   | <------------------- | Worker Manager 1 | 
    +----------+                       |                   |                      |                  |      +----------+
                                       |                   |                      |                  | ---> | Worker 2 |
    +----------+                       |                   |                      +------------------+      +----------+
    | Client B | --------------------> |     Scheduler     |
    +----------+                       |                   |                      +------------------+      +----------+
                                       |                   |                      |                  | ---> | Worker 3 |
    +----------+                       |                   |                      |                  |      +----------+
    | Client N | --------------------> |                   | <------------------- | Worker Manager 2 | 
    +----------+                       |                   |                      |                  |      +----------+
                                       |                   |                      |                  | ---> | Worker 4 |
                                       +-------------------+                      +------------------+      +----------+
                                         ^    ^    ^    ^    
                                         |    |    |    |
                                         +----|----|----|---------- all workers connect directly to scheduler


* Multiple clients can submit tasks to the same scheduler concurrently.
* The client is responsible for serializing tasks and arguments.
* Multiple worker managers can connect to the same scheduler and provision capacity in parallel.
* Worker managers spawn workers, and workers connect directly to the scheduler.
* The scheduler dispatches tasks to connected workers, and workers execute tasks and return results.

.. note::
    Although the architecture is similar to Dask, Scaler has a better decoupling of these systems and separation of concerns. For example, the Client only knows about the Scheduler and doesn't directly see the number of workers.

.. note::
    Scaler's Client is cross platform, supporting Windows and GNU/Linux, while other components can only be run on GNU/Linux.

.. _introduction_installation:

Installation
------------

The ``opengris-scaler`` package is available on PyPI and can be installed using any compatible package manager.

Base installation:

.. code:: bash

    pip install opengris-scaler

If you need the web GUI:

.. code:: bash

    pip install opengris-scaler[gui]

If you use GraphBLAS to solve DAG graph tasks:

.. code:: bash

    pip install opengris-scaler[graphblas]

If you need all optional dependencies:

.. code:: bash

    pip install opengris-scaler[all]

Key Features
------------

* Python ``multiprocessing``-style API, for example ``client.map()`` and ``client.submit()``.
* Graph tasks for DAG-based execution with explicit dependencies.
* Monitoring dashboard for real-time worker and task visibility.
* Task profiling for runtime and resource diagnostics.


First Look (Code API)
---------------------

Client.map
----------

:py:func:`~Client.map()` allows us to submit a batch of tasks to execute in parallel by pairing a function with a list of inputs.

In the example below, we spin up a scheduler and some workers on the local machine using ``SchedulerClusterCombo``. We create the scheduler with a localhost address, and then pass that address to the client so that it can connect. We then use :py:func:`~Client.map()` to submit tasks.

.. literalinclude:: ../../../examples/map_client.py
   :language: python


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

    scaler_cluster -n 10 tcp://127.0.0.1:8516


From here, connect the Python Client and begin submitting tasks:

.. code:: python

    from scaler import Client


    def square(value):
        return value * value


    with Client(address="tcp://127.0.0.1:8516") as client:
        results = client.map(square, range(0, 100))

    print(results)
