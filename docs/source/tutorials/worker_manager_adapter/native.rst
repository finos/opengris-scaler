Native Worker Manager
=====================

The Native worker manager provisions workers as local subprocesses on the same machine where the manager is running. It supports both dynamic auto-scaling (default) and fixed-pool mode, where a set number of workers are pre-spawned at startup.

Getting Started
---------------

To start the Native worker manager, use the ``scaler_worker_manager_baremetal_native`` command.

Example command:

.. code-block:: bash

    scaler_worker_manager_baremetal_native tcp://<SCHEDULER_IP>:8516 \
        --max-workers 4 \
        --logging-level INFO \
        --task-timeout-seconds 60

Equivalent configuration using a TOML file:

.. code-block:: bash

    scaler_worker_manager_baremetal_native tcp://<SCHEDULER_IP>:8516 --config config.toml

.. code-block:: toml

    # config.toml

    [native_worker_manager]
    max_workers = 4
    logging_level = "INFO"
    task_timeout_seconds = 60

*   ``tcp://<SCHEDULER_IP>:8516`` is the address workers will use to connect to the scheduler.
*   The manager can spawn up to 4 worker subprocesses in dynamic mode.

To use fixed-pool mode, set ``--mode fixed`` and specify the exact number of workers:

.. code-block:: toml

    # config.toml

    [native_worker_manager]
    mode = "fixed"
    max_workers = 8

How it Works
------------

**Dynamic mode** (default): when the scheduler determines that more capacity is needed, it sends a request to the Native worker manager. The manager then spawns a new worker process using the same Python interpreter and environment that started the manager.

**Fixed mode**: all workers are pre-spawned at startup. The manager runs a simple synchronous loop with no event loop or scheduler connector — it spawns the workers and waits for them to finish. Workers connect directly to the scheduler themselves. When all pre-spawned workers have exited, the manager itself exits.

Supported Parameters
--------------------

.. note::
    For more details on how to configure Scaler, see the :doc:`../configuration` section.

The Native worker manager supports the following specific configuration parameters in addition to the common worker manager parameters.

Native Configuration
~~~~~~~~~~~~~~~~~~~~

*   ``--worker-manager-id`` (``-wmi``): **Required.** A non-empty, globally unique string that identifies
    this worker manager to the scheduler. The scheduler uses this ID for scaling decisions, worker
    tracking, and duplicate detection. Must be unique across all managers connected to the same scheduler.
*   ``--mode``: Operating mode. ``dynamic`` (default) enables auto-scaling driven by the scheduler.
    ``fixed`` pre-spawns ``--max-workers`` workers at startup and does not support dynamic scaling.
    In fixed mode ``--max-workers`` must be a positive integer.
*   ``--worker-type``: Optional string prefix used in worker IDs. Overrides the default prefix (``NAT``
    for dynamic mode, ``FIX`` for fixed mode). Useful when multiple adapters of the same mode are
    running concurrently and their workers need to be distinguishable by type in logs and monitoring.
    Note: this controls the worker *process name* prefix, not the manager identity.
*   ``--max-workers`` (``-mw``): In dynamic mode, the maximum number of worker subprocesses that can be started (``-1`` = unlimited, default: ``-1``). In fixed mode, the exact number of workers spawned at startup (must be ≥ 1).
*   ``--preload``: Python module or script to preload in each worker process before it starts accepting tasks.
*   ``--worker-io-threads`` (``-wit``): Number of IO threads for the IO backend per worker (default: ``1``).

Common Parameters
~~~~~~~~~~~~~~~~~

For a full list of common parameters including networking, worker configuration, and logging, see :doc:`common_parameters`.
