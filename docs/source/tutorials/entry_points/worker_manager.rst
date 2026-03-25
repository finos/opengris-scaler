Unified Worker Manager Entry Point
===================================

``scaler_worker_manager`` is a single command that replaces the individual per-adapter entry points
(``scaler_worker_manager_baremetal_native``, ``scaler_worker_manager_symphony``, etc.).

.. note::
    ``scaler_cluster`` is no longer available. Users should migrate to
    ``scaler_worker_manager baremetal_native --mode fixed``.

Usage
-----

.. code-block:: bash

    scaler_worker_manager <subcommand> [options]

Available sub-commands: ``baremetal_native``, ``symphony``, ``aws_raw_ecs``, ``aws_hpc``.

The ``--config`` flag may appear before or after the sub-command name:

.. code-block:: bash

    scaler_worker_manager baremetal_native --config cluster.toml tcp://127.0.0.1:8516
    scaler_worker_manager --config cluster.toml baremetal_native tcp://127.0.0.1:8516

Logging
-------

The ``--logging-level``, ``--logging-paths``, and ``--logging-config-file`` flags control logging for
the worker manager process. They may appear before or after the sub-command name:

.. code-block:: bash

    scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 \
        --worker-manager-id wm-1 \
        --logging-level DEBUG \
        --logging-paths /var/log/scaler/wm.log

Event Loop and IO Threads
--------------------------

``--event-loop`` and ``--io-threads`` are worker-level settings passed to every worker spawned by
the manager. They appear under the sub-command:

.. code-block:: bash

    scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 \
        --worker-manager-id wm-1 \
        --event-loop builtin \
        --io-threads 2

Using a TOML Config File
-------------------------

All flags can be placed in a TOML file. All settings — including logging and worker settings —
go in the sub-command section:

.. code-block:: toml

    # cluster.toml
    [baremetal_native]
    scheduler_address = "tcp://127.0.0.1:8516"
    worker_manager_id = "wm-1"
    max_task_concurrency = 4
    logging_level = "INFO"
    event_loop = "builtin"
    io_threads = 2

.. code-block:: bash

    scaler_worker_manager baremetal_native --config cluster.toml tcp://127.0.0.1:8516

CLI flags always override TOML values, so ``--logging-level DEBUG`` takes precedence over
``logging_level = "INFO"`` in the config file.

Sub-commands
------------

baremetal_native
~~~~~~~~~~~~~~~~

Provisions workers as local subprocesses. Supports dynamic (default) and fixed-pool mode.

.. code-block:: bash

    # Dynamic mode (auto-scaling)
    scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 \
        --worker-manager-id wm-1 \
        --max-task-concurrency 4

    # Fixed mode (pre-spawned workers)
    scaler_worker_manager baremetal_native --mode fixed \
        --max-task-concurrency 4 \
        --worker-manager-id wm-1 \
        tcp://127.0.0.1:8516

See :doc:`../worker_managers/baremetal_native` for full parameter details.

symphony
~~~~~~~~

Integrates with IBM Spectrum Symphony.

.. code-block:: bash

    scaler_worker_manager symphony tcp://127.0.0.1:8516 \
        --worker-manager-id wm-1 \
        --service-name ScalerService

aws_raw_ecs
~~~~~~~~~~~

Provisions workers as AWS ECS Fargate tasks.

.. code-block:: bash

    scaler_worker_manager aws_raw_ecs tcp://127.0.0.1:8516 \
        --worker-manager-id wm-1 \
        --ecs-cluster my-cluster \
        --ecs-task-image my-image:latest \
        --aws-region us-east-1

aws_hpc
~~~~~~~

Provisions workers via AWS Batch.

.. code-block:: bash

    scaler_worker_manager aws_hpc tcp://127.0.0.1:8516 \
        --worker-manager-id wm-1 \
        --job-queue my-queue \
        --job-definition my-job-def \
        --s3-bucket my-bucket

See :doc:`../worker_managers/aws_hpc_batch` for full parameter details.
