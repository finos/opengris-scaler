Common Worker Manager Parameters
================================

All worker managers in Scaler share a set of common configuration parameters. Not every worker manager supports every parameter — specific docs note any differences.

.. note::
    For more details on Scaler configuration, see :doc:`../commands`.

Networking
----------

* ``scheduler_address`` (positional, required): The address of the scheduler (e.g., ``tcp://127.0.0.1:8516``).
* ``--worker-manager-id`` (``-wmi``, required): A stable identifier for this worker manager instance. Must be unique across all managers connected to the same scheduler. The scheduler uses this ID to associate workers with their manager and to detect duplicate connections.
* ``--max-task-concurrency`` (``-mtc``): Maximum number of workers that can be started (default: number of CPUs − 1). Set to ``-1`` for no limit.
* ``--object-storage-address`` (``-osa``): Address of the object storage server (e.g., ``tcp://127.0.0.1:8517``). If not set, defaults to the scheduler address with port + 1.
* ``--config`` (``-c``): Path to a TOML configuration file.

Worker Behavior
---------------

These parameters control individual worker processes started by the worker manager.

* ``--per-worker-task-queue-size`` (``-wtqs``): Task queue size per worker (default: ``1000``).
* ``--heartbeat-interval-seconds`` (``-his``): Interval at which workers send heartbeats to the scheduler in seconds (default: ``2``).
* ``--task-timeout-seconds`` (``-tts``): Seconds before a task is considered timed out. ``0`` means no timeout (default: ``0``).
* ``--death-timeout-seconds`` (``-dts``): Seconds before a worker is considered dead if no heartbeat is received (default: ``300``).
* ``--garbage-collect-interval-seconds`` (``-gc``): Interval at which the worker runs garbage collection in seconds (default: ``30``).
* ``--trim-memory-threshold-bytes`` (``-tm``): Threshold for trimming libc's memory in bytes (default: ``1073741824``, i.e., 1 GB).
* ``--hard-processor-suspend`` (``-hps``): When set, suspends worker processors using SIGTSTP instead of a synchronization event.
* ``--io-threads`` (``-it``): Number of IO threads per worker (default: ``1``).
* ``--per-worker-capabilities`` (``-pwc``): Comma-separated list of capabilities (e.g., ``"linux,cpu=4"``).

Logging and Event Loop
----------------------

* ``--event-loop`` (``-el``): Event loop type: ``builtin`` or ``uvloop`` (default: ``builtin``).
* ``--logging-level`` (``-ll``): Logging level: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL`` (default: ``WARNING``).
* ``--logging-paths`` (``-lp``): Paths where logs are written. Defaults to ``/dev/stdout``. Multiple paths can be specified.
* ``--logging-config-file`` (``-lcf``): Path to a Python logging configuration file (``.conf`` format).
