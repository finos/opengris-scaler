Client Mapping Tasks
====================

Use :py:func:`~Client.map()` to submit the same function across many inputs in parallel.

In the example below, a local scheduler and workers are started with
``SchedulerClusterCombo``. The client then connects to the scheduler address and calls
:py:func:`~Client.map()`.

.. literalinclude:: ../../../../examples/map_client.py
   :language: python

What the example does:

* Starts a local scheduler + workers with ``SchedulerClusterCombo``.
* Uses :py:func:`~Client.map()` to run ``math.sqrt`` across ``range(100)``.
* Uses :py:func:`~Client.map()` with two iterables for pairwise arguments.
* Uses :py:func:`~Client.starmap()` when arguments are already grouped as tuples.

When to choose each API:

* Use :py:func:`~Client.map()` when arguments come from one or more parallel iterables.
* Use :py:func:`~Client.starmap()` when each task input is a prebuilt argument tuple.
