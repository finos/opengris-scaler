Fixed Native Worker Manager
===========================

The Fixed Native worker manager spawns a fixed number of worker subprocesses at startup. Unlike other worker managers, it does **not** support dynamic scaling. It is useful for environments where you want a static pool of workers to be available immediately and do not want the system to dynamically adjust the number of processes.

Getting Started
---------------

To start the Fixed Native worker manager, use the ``scaler_worker_manager_baremetal_fixed_native`` command.

Example command:

.. code-block:: bash

    scaler_worker_manager_baremetal_fixed_native tcp://<SCHEDULER_IP>:8516 \
        --max-workers 8 \
        --logging-level INFO \
        --task-timeout-seconds 60

Equivalent configuration using a TOML file:

.. code-block:: bash

    scaler_worker_manager_baremetal_fixed_native tcp://<SCHEDULER_IP>:8516 --config config.toml

.. code-block:: toml

    # config.toml

    [fixed_native_worker_manager]
    max_workers = 8
    logging_level = "INFO"
    task_timeout_seconds = 60

*   ``tcp://<SCHEDULER_IP>:8516`` is the address workers will use to connect to the scheduler.
*   The manager will immediately spawn 8 worker subprocesses at startup and maintain them.

How it Works
------------

Upon startup, the Fixed Native worker manager spawns the number of workers specified by ``--max-workers``. It reports its capacity to the scheduler as 0 to prevent the scheduler from attempting to scale it up or down dynamically.

If a worker process terminates, the manager does not automatically restart it (in the current implementation).

Supported Parameters
--------------------

.. note::
    For more details on how to configure Scaler, see the :doc:`../configuration` section.

The Fixed Native worker manager supports the following specific configuration parameters in addition to the common worker manager parameters.

Fixed Native Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~

*   ``--max-workers`` (``-mw``): The exact number of worker subprocesses to spawn at startup. Must be a non-negative integer.
*   ``--preload``: Python module or script to preload in each worker process before it starts accepting tasks.
*   ``--worker-io-threads`` (``-wit``): Number of IO threads for the IO backend per worker (default: ``1``).

Common Parameters
~~~~~~~~~~~~~~~~~

For a full list of common parameters including networking, worker configuration, and logging, see :doc:`common_parameters`.
