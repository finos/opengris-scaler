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

Reusing the same object across tasks
------------------------------------

When the same Python object is passed to several tasks, Scaler serializes and uploads it only once
and reuses that upload for every task that references it -- whether the tasks come from a single
:py:func:`~Client.map()` / :py:func:`~Client.get()` call or from many separate
:py:func:`~Client.submit()` calls. This happens automatically; no code change is required.

Two layers cooperate here. A client-side cache keyed by object identity (``id(obj)``) avoids
re-serializing the same object, and object IDs are *content-addressed* (derived from the serialized
bytes), so the upload itself is skipped whenever the server already holds identical content.

There is one caveat: if you **mutate an object in place** and submit it again, the identity cache
returns the pre-mutation snapshot and the task receives the stale bytes. Pass ``reserialize=True`` to
re-serialize that call's arguments and refresh the cache:

.. code:: python

    data = load_dataframe()
    client.submit_verbose(train, (data,), {})                    # serialized, uploaded, cached

    data.drop(columns=["unused"], inplace=True)                  # mutated in place
    client.submit_verbose(train, (data,), {}, reserialize=True)  # re-serialized and re-uploaded

Because IDs are content-addressed, ``reserialize`` re-serializes the call's arguments but only
re-uploads the ones whose content actually changed -- passing it for an object that turned out not to
have changed costs a re-serialization, not a re-upload.

``reserialize`` is available on :py:func:`~Client.submit_verbose()`, :py:func:`~Client.map()`,
:py:func:`~Client.starmap()` and :py:func:`~Client.get()`. :py:func:`~Client.submit()` forwards its
keyword arguments to your function, so use :py:func:`~Client.submit_verbose()` when you need the
flag. It affects only the objects in that one call; every other cached object is untouched.

Sending a heavy object explicitly
---------------------------------

Because reuse is deduplicated automatically (above), you rarely need to send objects by hand.
:py:func:`~Client.send_object()` still helps in one case: it serializes a large payload **once** and
returns a lightweight reference, avoiding the per-call re-serialization that the automatic cache
cannot skip for non-weakref-able built-ins (``bytes``, ``str``, ``list``, ``dict``, ``tuple``) reused
across many separate :py:func:`~Client.submit()` calls. It is also an explicit handle you can pass
wherever a positional argument is expected.

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

Notes for :py:func:`~Client.send_object()`:

* The payload is serialized and uploaded once; each task then carries only a small reference.
* Unlike passing the object directly, the reference is not re-serialized per task.
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
