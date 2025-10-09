Quickstart
==========


When to use it
--------------

Scaler is inspired by Dask and functions like other parallel backends. It handles the communication between the Client, Scheduler, and Workers to orchestrate the execution of tasks. It is a good fit for scaling compute-heavy jobs across multiple machines, or even on a local machine using process-level parallelization.

Architecture
------------

Below is a diagram of the relationship between the Client, Scheduler, and Workers.

.. image:: images/architecture.png
   :width: 600


* The Client submits tasks to the scheduler. This is the primary user-facing API.
* The Client is responsible for serializing the tasks
* The Scheduler receives tasks from the client and distributes the tasks among the workers
* Workers perform the computation and return the results

.. note::
    Although the architecture is similar to Dask, Scaler has a better decoupling of these systems and separation of concerns. For example, the Client only knows about the Scheduler and doesn't directly see the number of workers.

.. note::
    Scaler's Client is cross platform, supporting Windows and GNU/Linux, while other components can only be run on GNU/Linux.


Installation
------------

The `scaler` package is available on PyPI and can be installed using any compatible package manager.

.. code:: bash

    pip install opengris-scaler

    # or with graphblas and uvloop and webui support
    pip install opengris-scaler[graphblas,uvloop,gui]

    # or install all dependencies
    pip install opengris-scaler[all]


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

    scaler_worker -n 10 tcp://127.0.0.1:8516


.. code:: console

    [INFO]2023-03-19 12:19:19-0400: logging to ('/dev/stdout',)
    [INFO]2023-03-19 12:19:19-0400: ClusterProcess: starting 10 workers, heartbeat_interval_seconds=2, object_retention_seconds=3600
    [INFO]2023-03-19 12:19:19-0400: Worker[0] started
    [INFO]2023-03-19 12:19:19-0400: Worker[1] started
    [INFO]2023-03-19 12:19:19-0400: Worker[2] started
    [INFO]2023-03-19 12:19:19-0400: Worker[3] started
    [INFO]2023-03-19 12:19:19-0400: Worker[4] started
    [INFO]2023-03-19 12:19:19-0400: Worker[5] started
    [INFO]2023-03-19 12:19:19-0400: Worker[6] started
    [INFO]2023-03-19 12:19:19-0400: Worker[7] started
    [INFO]2023-03-19 12:19:19-0400: Worker[8] started
    [INFO]2023-03-19 12:19:19-0400: Worker[9] started


From here, connect the Python Client and begin submitting tasks:

.. code:: python

    from scaler import Client


    def square(value):
        return value * value


    with Client(address="tcp://127.0.0.1:8516") as client:
        results = client.map(square, [(i,) for i in range(0, 100)])

    print(results)
