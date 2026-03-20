All-in-One Entry Point (scaler_all)
====================================

``scaler_all`` boots the full Scaler stack — object storage server, scheduler, and one or more worker managers —
from a single TOML configuration file. Each recognised section spawns a separate process. Processes are started
in dependency order: object storage first, then the scheduler (which receives the object storage address), then
worker managers (which receive the scheduler address and object storage address).

Usage
-----

.. code-block:: bash

    scaler_all --config <file>

If no recognised sections are found, ``scaler_all`` exits with an error.

Minimal Example
---------------

The simplest useful configuration — object storage server, scheduler, and one native worker manager.
All network addresses are chosen automatically by the OS:

.. code-block:: toml

    [object_storage_server]

    [scheduler]

    [[worker_manager]]
    type = "baremetal_native"
    worker_manager_id = "wm-1"

.. code-block:: bash

    scaler_all --config stack.toml

Extended Example
----------------

The optional ``[logging]`` section configures logging for worker manager processes. The optional ``[worker]``
section sets worker-level options such as the event loop and number of IO threads, shared across all worker
managers. Use the ``[[worker_manager]]`` array-of-tables syntax to define multiple worker managers; each entry
is fully independent and can have a different ``type``, so you can mix adapter types in a single deployment.
Here the object storage server and scheduler are bound to explicit addresses on all interfaces so that
remote worker managers (ECS, Batch) can reach them by the host's routable IP:

.. code-block:: toml

    [logging]
    level = "INFO"
    paths = ["/var/log/scaler/worker.log"]

    [worker]
    event_loop = "builtin"
    io_threads = 2

    [object_storage_server]
    object_storage_address = "tcp://0.0.0.0:6379"

    [scheduler]
    scheduler_address = "tcp://0.0.0.0:6378"

    [[worker_manager]]
    type = "baremetal_native"
    worker_manager_id = "wm-native-1"
    scheduler_address = "tcp://127.0.0.1:6378"
    max_task_concurrency = 4
    mode = "fixed"

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

    scaler_all --config stack.toml

Object Storage
--------------

The ``[object_storage_server]`` section tells ``scaler_all`` to start and manage the object storage server.
The actual bound address is propagated automatically to the scheduler and all worker managers, so no
``object_storage_address`` key needs to appear in ``[scheduler]`` or ``[[worker_manager]]``.
Worker managers that omit ``object_storage_address`` connect on ``127.0.0.1`` at the object
storage server's actual bound port. Worker managers that set ``object_storage_address`` to port
``0`` keep their configured host with the port filled in from the actual bound port.

If a specific address is required (e.g. so an external component can connect to it), set
``object_storage_address`` explicitly:

.. code-block:: toml

    [object_storage_server]
    object_storage_address = "tcp://0.0.0.0:6379"

If ``[object_storage_server]`` is absent, no object storage server is started by ``scaler_all``. In that case
both the ``[scheduler]`` section and every ``[[worker_manager]]`` section must include an
``object_storage_address`` pointing to an already-running server:

.. code-block:: toml

    [scheduler]
    object_storage_address = "tcp://127.0.0.1:6379"

    [[worker_manager]]
    type = "baremetal_native"
    worker_manager_id = "wm-1"
    object_storage_address = "tcp://127.0.0.1:6379"

Scheduler Address
-----------------

If ``scheduler_address`` is omitted from ``[scheduler]``, or set to port ``0`` (e.g.
``"tcp://0.0.0.0:0"``), the OS assigns a free port automatically.
Worker managers that omit ``scheduler_address`` connect to the scheduler on ``127.0.0.1`` at its
actual bound port. Worker managers that set ``scheduler_address`` to port ``0`` keep their
configured host but have the port filled in from the scheduler's actual bound port — useful when
a worker manager needs to reach the scheduler on a specific network interface
(e.g. ``"tcp://10.0.1.50:0"``).
In most deployments no address configuration is needed at all (as shown in the example above).

To pin the scheduler to a specific port:

.. code-block:: toml

    [scheduler]
    scheduler_address = "tcp://0.0.0.0:6378"

Setting ``scheduler_address`` to port ``0`` in a ``[[worker_manager]]`` entry lets you choose the
network interface while still relying on auto-assigned port discovery. This is useful when a single
host runs both local and remote worker managers at the same time:

.. code-block:: toml

    [object_storage_server]

    [scheduler]
    # port 0 → OS picks a free port on all interfaces

    [[worker_manager]]
    type = "baremetal_native"
    worker_manager_id = "wm-local"
    # scheduler_address omitted → connects on 127.0.0.1 at the actual bound port
    max_task_concurrency = 4

    [[worker_manager]]
    type = "symphony"
    worker_manager_id = "wm-remote"
    scheduler_address = "tcp://10.0.1.50:0"       # keeps routable host, port filled in
    object_storage_address = "tcp://10.0.1.50:0"  # same for object storage
    service_name = "ScalerService"
    max_task_concurrency = 8

Logging Configuration
---------------------

The optional ``[logging]`` section configures logging for all worker manager processes started by ``scaler_all``.
The scheduler process uses its own ``[scheduler]`` logging settings (see below).

.. code-block:: toml

    [logging]
    level = "DEBUG"
    paths = ["/var/log/scaler/worker.log"]

If ``[logging]`` is absent, default logging settings are used (stdout, ``INFO`` level).

Worker Configuration
--------------------

Worker settings such as ``event_loop`` and ``io_threads`` belong to the ``[worker]`` section,
which is shared across all worker managers started by ``scaler_all``:

.. code-block:: toml

    [worker]
    event_loop = "builtin"    # or "uvloop"
    io_threads = 2

Recognised Section Names
-------------------------

The following section names are recognised:

.. list-table::
   :header-rows: 1

   * - TOML Section
     - Component started
   * - ``[logging]``
     - Logging config for worker manager processes
   * - ``[object_storage_server]``
     - Object storage server
   * - ``[scheduler]``
     - Scaler scheduler
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
