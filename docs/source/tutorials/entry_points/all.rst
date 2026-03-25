All-in-One Entry Point (scaler)
===============================

``scaler`` boots the full Scaler stack — scheduler and one or more worker managers —
from a single TOML configuration file. Each recognised section spawns a separate process.
The scheduler is started first; worker managers follow once the scheduler process is running.

The scheduler also manages the object storage server. If ``object_storage_address`` is omitted
from ``[scheduler]``, the scheduler automatically spawns an object storage server on
``scheduler_address.port + 1``.

Usage
-----

.. code-block:: bash

    scaler <file>

If no recognised sections are found, ``scaler`` exits with an error.

Minimal Example
---------------

The simplest useful configuration — a scheduler and one native worker manager. Because
``object_storage_address`` is not set in ``[scheduler]``, the scheduler automatically starts
an object storage server on port 6379 (``scheduler_address.port + 1``):

.. code-block:: toml

    [scheduler]
    scheduler_address = "tcp://127.0.0.1:6378"

    [[worker_manager]]
    type = "baremetal_native"
    worker_manager_id = "wm-1"
    scheduler_address = "tcp://127.0.0.1:6378"

.. code-block:: bash

    scaler stack.toml

Extended Example
----------------

Use the ``[[worker_manager]]`` array-of-tables syntax to define multiple worker managers; each entry is fully
independent and can have a different ``type``, so you can mix adapter types in a single deployment. Each manager
carries its own ``logging_config`` and ``worker_config`` settings (e.g. ``event_loop``, ``io_threads``).

.. code-block:: toml

    [scheduler]
    scheduler_address = "tcp://0.0.0.0:6378"

    [[worker_manager]]
    type = "baremetal_native"
    worker_manager_id = "wm-native-1"
    scheduler_address = "tcp://127.0.0.1:6378"
    max_task_concurrency = 4
    mode = "fixed"
    event_loop = "builtin"
    io_threads = 2
    logging_level = "INFO"
    logging_paths = ["/var/log/scaler/worker.log"]

    [[worker_manager]]
    type = "baremetal_native"
    worker_manager_id = "wm-native-2"
    scheduler_address = "tcp://127.0.0.1:6378"
    max_task_concurrency = 8
    mode = "dynamic"
    preload = "mypackage.init:setup"

    # ECS and Batch workers run on remote machines, so they need routable
    # scheduler and object storage server addresses — not the loopback address
    # used by local managers.
    [[worker_manager]]
    type = "aws_raw_ecs"
    worker_manager_id = "wm-ecs-1"
    scheduler_address = "tcp://10.0.1.50:6378"
    object_storage_address = "tcp://10.0.1.50:6379"
    max_task_concurrency = 50
    ecs_cluster = "scaler-cluster"
    ecs_task_definition = "scaler-task-definition"
    ecs_subnets = "subnet-0abc1234,subnet-0def5678"
    ecs_task_cpu = 4
    ecs_task_memory = 30

    [[worker_manager]]
    type = "aws_hpc"
    worker_manager_id = "wm-batch-1"
    scheduler_address = "tcp://10.0.1.50:6378"
    object_storage_address = "tcp://10.0.1.50:6379"
    max_task_concurrency = 100
    job_queue = "scaler-job-queue"
    job_definition = "scaler-job-definition"
    s3_bucket = "my-scaler-bucket"

.. code-block:: bash

    scaler stack.toml

Object Storage
--------------

The object storage server is managed by the scheduler, not by ``scaler`` directly.

If ``object_storage_address`` is omitted from ``[scheduler]``, the scheduler automatically starts
an object storage server on the same host as the scheduler, at ``scheduler_address.port + 1``:

.. code-block:: toml

    [scheduler]
    scheduler_address = "tcp://0.0.0.0:6378"
    # object storage server will be started automatically on port 6379

If you need to use a specific object storage address (e.g. an already-running server, or a
different port), set ``object_storage_address`` explicitly in ``[scheduler]``:

.. code-block:: toml

    [scheduler]
    scheduler_address = "tcp://0.0.0.0:6378"
    object_storage_address = "tcp://127.0.0.1:9000"

Worker managers that require access to the object storage server (e.g. remote ECS or Batch
workers) must set ``object_storage_address`` explicitly in their ``[[worker_manager]]`` entry.
Local worker managers that only communicate with the scheduler do not need it.

Scheduler Address
-----------------

``scheduler_address`` is required in both the ``[scheduler]`` section and every ``[[worker_manager]]``
section. There is no automatic address discovery or propagation between sections.

.. code-block:: toml

    [scheduler]
    scheduler_address = "tcp://0.0.0.0:6378"

    [[worker_manager]]
    type = "baremetal_native"
    worker_manager_id = "wm-1"
    scheduler_address = "tcp://127.0.0.1:6378"

Recognised Section Names
-------------------------

The following section names are recognised:

.. list-table::
   :header-rows: 1

   * - TOML Section
     - Component started
   * - ``[scheduler]``
     - Scaler scheduler (also manages object storage server)
   * - ``[[worker_manager]]``
     - Worker manager (discriminated by ``type`` field)

Worker manager ``type`` values:

.. list-table::
   :header-rows: 1

   * - ``type``
     - Adapter
   * - ``"baremetal_native"``
     - Native (local subprocess) manager
   * - ``"symphony"``
     - IBM Symphony manager
   * - ``"aws_raw_ecs"``
     - AWS ECS manager
   * - ``"aws_hpc"``
     - AWS Batch manager
