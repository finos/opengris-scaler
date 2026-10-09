Common Worker Manager Parameters
================================

All worker managers in Scaler share a set of common configuration parameters. Not every worker manager supports every parameter — specific docs note any differences.

.. note::
    For more details on Scaler configuration, see :doc:`../commands`.

Networking
----------

* ``scheduler_address`` (positional, required): The address of the scheduler (e.g., ``tcp://127.0.0.1:8516``).
* ``--worker-scheduler-address`` (``-wsa``): Scheduler address used by spawned workers. If omitted, it defaults to ``scheduler_address``.
* ``--worker-manager-id`` (``-wmi``, required): A stable identifier for this worker manager instance. Must be unique across all managers connected to the same scheduler. The scheduler uses this ID to associate workers with their manager and to detect duplicate connections.
* ``--max-task-concurrency`` (``-mtc``): Maximum total number of workers that can be active across all spawned instances or containers (default: number of CPUs). Set to ``-1`` for no limit. For worker managers that spawn multi-worker machines (e.g. ORB AWS EC2, AWS Raw ECS), the number of instances spawned is ``ceil(max_task_concurrency / workers_per_instance)``. Every instance runs ``workers_per_instance`` workers except the last, which runs the remainder.

  **Example** — ``--max-task-concurrency 10`` with an EC2 instance type that has 4 vCPUs (e.g. ``c5.xlarge``):

  .. math::

      \lceil 10 / 4 \rceil = 3 \text{ instances, running } 4 + 4 + 2 = 10 \text{ workers}

  The last instance is billed in full although it runs only 2 workers.
* ``--object-storage-address`` (``-osa``): Optional object storage server address override (e.g., ``tcp://127.0.0.1:8517``). If omitted, workers use the address advertised by scheduler heartbeats.
* ``--config`` (``-c``): Path to a TOML configuration file.

Draining and Nesting
--------------------

A worker manager retires a unit by draining it: the unit takes no new task, finishes its running tasks, then exits.

* ``--drain-timeout-seconds`` (``-drt``): Seconds a unit may take to finish its running tasks after it is told to drain, before the worker manager destroys it by force (default: ``300``).
* ``--unit-timeout-seconds`` (``-uts``): Seconds a unit may go without a heartbeat before the worker manager counts it as lost, destroys it by force, and replaces it (default: ``60``). A worker or a child worker manager that sends no heartbeat, such as a worker that was killed or is deadlocked, is caught this way. A new unit has longer to send its first heartbeat: 60 seconds for a local process, 600 seconds for a cloud resource.
* ``--children-address`` (``-ca``): Address the worker manager binds for its units to dial. Local worker processes default to a free loopback port. ORB AWS EC2, AWS Raw ECS, and OCI Raw require it: each provisioned resource runs a native worker manager that dials this address, so it must be reachable from those resources.
* ``--parent-address`` (``-pa``) and ``--unit-id``: Set by a cloud worker manager in the command that starts its child. The child takes its desired task concurrency from this parent instead of the scheduler.

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
  Each entry is either a bare capability name (e.g., ``linux``, meaning unlimited) or ``name=integer`` (e.g., ``cpu=4``).
  Capability names must not be empty strings: entries like ``=1`` or a value without an integer after ``=``
  (e.g., ``cpu=``) are rejected.

Logging and Event Loop
----------------------

* ``--event-loop`` (``-el``): Event loop type: ``builtin`` or ``uvloop`` (default: ``builtin``).
* ``--logging-level`` (``-ll``): Logging level: ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL`` (default: ``WARNING``).
* ``--logging-paths`` (``-lp``): Paths where logs are written. Defaults to ``/dev/stdout``. Multiple paths can be specified.
* ``--logging-config-file`` (``-lcf``): Path to a Python logging configuration file (``.conf`` format).
