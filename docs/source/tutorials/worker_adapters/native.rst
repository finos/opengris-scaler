Native Worker Adapter
=====================

The Native worker adapter allows Scaler to dynamically provision workers as local subprocesses on the same machine where the adapter is running. This is the simplest way to scale Scaler workloads across multiple CPU cores on a single machine or a group of machines.

Getting Started
---------------

To start the Native worker adapter, use the ``scaler_worker_adapter_native`` command.

Example command:

.. code-block:: bash

    scaler_worker_adapter_native tcp://<SCHEDULER_IP>:8516 \
        --adapter-web-host 0.0.0.0 \
        --adapter-web-port 8080 \
        --max-workers 4 \
        --logging-level INFO \
        --task-timeout-seconds 60

Equivalent configuration using a TOML file:

.. code-block:: bash

    scaler_worker_adapter_native tcp://<SCHEDULER_IP>:8516 --config config.toml

.. code-block:: toml

    # config.toml

    [native_worker_adapter]
    adapter_web_host = "0.0.0.0"
    adapter_web_port = 8080
    max_workers = 4
    logging_level = "INFO"
    task_timeout_seconds = 60

*   ``tcp://<SCHEDULER_IP>:8516`` is the address workers will use to connect to the scheduler.
*   The adapter HTTP server (webhook) will listen on port 8080. The scheduler connects to this.
*   The adapter can spawn up to 4 worker subprocesses.

How it Works
------------

When the scheduler determines that more capacity is needed, it sends a request to the Native worker adapter. The adapter then spawns a new worker process using the same Python interpreter and environment that started the adapter.

Each worker group managed by the Native adapter contains exactly one worker process.

Unlike the Fixed Native worker adapter, which spawns a static number of workers at startup, the Native worker adapter is designed to be used with Scaler's auto-scaling features to dynamically grow and shrink the local worker pool based on demand.

Supported Parameters
--------------------

.. note::
    For more details on how to configure Scaler, see the :doc:`../configuration` section.

The Native worker adapter supports the following specific configuration parameters in addition to the common worker adapter parameters.

Native Configuration
~~~~~~~~~~~~~~~~~~~~

*   ``--max-workers`` (``-mw``): Maximum number of worker subprocesses that can be started. Set to ``-1`` for no limit (default: ``-1``).
*   ``--preload``: Python module or script to preload in each worker process before it starts accepting tasks.
*   ``--worker-io-threads`` (``-wit``): Number of IO threads for the IO backend per worker (default: ``1``).

Common Parameters
~~~~~~~~~~~~~~~~~

For a full list of common parameters including networking, worker configuration, and logging, see :doc:`common_parameters`.
