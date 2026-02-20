Common Worker Adapter Parameters
================================

All worker adapters in Scaler share a set of common configuration parameters for connecting to the cluster, configuring the internal web server, and managing worker behavior.

.. note::
    For more details on how to configure Scaler, see the :doc:`../configuration` section.

Worker Adapter Common Configuration
-----------------------------------

*   ``scheduler_address`` (Positional, Required): The address of the scheduler that workers should connect to (e.g., ``tcp://127.0.0.1:8516``).
*   ``--max-workers`` (``-mw``): Maximum number of workers that can be started (default: ``CPU_COUNT - 1``). Set to ``-1`` for no limit.
*   ``--object-storage-address`` (``-osa``): Address of the object storage server (e.g., ``tcp://127.0.0.1:8517``).

Web Configuration
-----------------

Each worker adapter runs an internal HTTP server that the scheduler uses to send scaling requests.

*   ``--adapter-web-host`` (Required): Host address for the worker adapter HTTP server (e.g., ``0.0.0.0``).
*   ``--adapter-web-port`` (``-p``, Required): Port for the worker adapter HTTP server.

Worker Configuration (Passed to Workers)
----------------------------------------

These parameters are passed to the individual worker processes started by the adapter.

*   ``--per-worker-task-queue-size`` (``-wtqs``): Set the task queue size per worker (default: 1000).
*   ``--heartbeat-interval-seconds`` (``-his``): The interval at which workers send heartbeats to the scheduler (default: 2).
*   ``--task-timeout-seconds`` (``-tts``): Number of seconds before a task is considered timed out. 0 means no timeout (default: 0).
*   ``--death-timeout-seconds`` (``-dts``): Number of seconds before a worker is considered dead if no heartbeat is received (default: 300).
*   ``--garbage-collect-interval-seconds`` (``-gc``): Interval at which the worker runs its garbage collector (default: 30).
*   ``--trim-memory-threshold-bytes`` (``-tm``): Threshold for trimming libc's memory (default: 1073741824, i.e., 1GB).
*   ``--hard-processor-suspend`` (``-hps``): When set, suspends worker processors using the SIGTSTP signal instead of a synchronization event.
*   ``--worker-io-threads`` (``-wit``): Set the number of IO threads for the IO backend per worker (default: 1).
*   ``--per-worker-capabilities`` (``-pwc``): A comma-separated list of capabilities provided by the workers (e.g., ``"linux,cpu=4"``).

Logging and Event Loop
----------------------

*   ``--event-loop`` (``-el``): Select the event loop type (e.g., ``builtin``, ``uvloop``).
*   ``--logging-level`` (``-ll``): Set the logging level (e.g., ``DEBUG``, ``INFO``, ``WARNING``, ``ERROR``, ``CRITICAL``).
*   ``--logging-paths`` (``-lp``): List of paths where the logs should be written. Defaults to ``/dev/stdout``.
*   ``--logging-config-file`` (``-lcf``): Path to a Python logging configuration file (``.conf`` format).
