Client Submit Tasks
===================

Use :py:func:`~Client.submit()` for one task (function + arguments).
Call ``result()`` to retrieve the value.

.. literalinclude:: ../../../../examples/simple_client.py
   :language: python

What the example does:

* Starts a local scheduler + workers with ``SchedulerClusterCombo``.
* Connects a client to that scheduler address.
* Calls :py:func:`~Client.submit()` once per task input.
* Resolves each returned future with ``result()`` and aggregates the values.

Use ``submit`` when you need one-off calls or per-task argument differences.

Heavy argument anti-pattern
---------------------------

If many tasks share the same large payload, repeated :py:func:`~Client.submit()` calls can be slow.
Each call may resend and reserialize that payload.

.. testcode:: python

    import functools
    import random

    from scaler import Client, SchedulerClusterCombo

    def lookup(heavy_map: bytes, index: int):
        return index * 1


    def main():
        address = "tcp://127.0.0.1:2345"
        cluster = SchedulerClusterCombo(address=address, n_workers=3)
        big_func = functools.partial(lookup, b"1" * 10_000_000)
        arguments = [random.randint(0, 100) for _ in range(100)]

        with Client(address=address) as client:
            futures = [client.submit(big_func, i) for i in arguments]
            print([fut.result() for fut in futures])

        cluster.shutdown()


    if __name__ == "__main__":
        main()


Why this is slow:

* ``big_func`` captures a large object.
* Every ``submit`` can trigger repeated serialization and network transfer.
* Serialization and I/O overhead can dominate task runtime.

Better pattern: send heavy object once
--------------------------------------

Send the heavy object once with :py:func:`~Client.send_object()`, then submit tasks with its reference.

.. testcode:: python

    import random

    from scaler import Client, SchedulerClusterCombo


    def lookup(heavy_map_ref, index: int):
        return heavy_map_ref[index]


    def main():
        address = "tcp://127.0.0.1:2345"
        cluster = SchedulerClusterCombo(address=address, n_workers=3)
        heavy_map = b"1" * 10_000_000
        arguments = [random.randint(0, 100) for _ in range(100)]

        with Client(address=address) as client:
            heavy_map_ref = client.send_object(heavy_map, name="heavy_map")
            futures = [client.submit(lookup, heavy_map_ref, i) for i in arguments]
            print([future.result() for future in futures])

        cluster.shutdown()


    if __name__ == "__main__":
        main()

Why this is better:

* The large payload is transferred once.
* Each task carries only a small reference + lightweight args.
* Lower serialization and network I/O typically improves throughput and latency.

Notes for :py:func:`~Client.send_object()`:

* The object is uploaded once and reused by many tasks.
* The returned reference must be passed as a positional function argument.
* Do not nest object references inside other containers (for example lists or dicts).

Task profiling
--------------

To measure per-task runtime and memory, enable profiling when submitting the task.
Task profiling values are available after the task completes.

.. code:: python

    from scaler import Client


    def calculate(sec: int):
        return sec * 1


    client = Client(address="tcp://127.0.0.1:2345")
    fut = client.submit(calculate, 1, profiling=True)

    # Ensure task execution is complete
    fut.result()

    # Runtime in microseconds
    fut.profiling_info().duration_us

    # Peak task memory usage in bytes (sampled periodically)
    fut.profiling_info().peak_memory
