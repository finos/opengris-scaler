Baremetal Native Worker Manager
===============================

The Baremetal Native worker manager spawns worker subprocesses on the local machine. It is the simplest way to run Scaler across multiple CPU cores and is the recommended starting point for most users.

The scheduler's scaling policy decides how many workers it runs. For a fixed pool, pair it with ``scaling=static``, which always asks for ``--max-task-concurrency`` workers.

Quick Start (Python API)
------------------------

The fastest way to get going is with ``SchedulerClusterCombo``, which starts a scheduler, object storage server, and a worker manager that runs ``n_workers`` workers, in a single Python process:

.. code-block:: python

   from scaler import Client, SchedulerClusterCombo

   def add(a, b):
       return a + b

   if __name__ == "__main__":
       cluster = SchedulerClusterCombo(address="tcp://127.0.0.1:8516", n_workers=4)

       with Client(address="tcp://127.0.0.1:8516") as client:
           future = client.submit(add, 2, 3)
           print(future.result())  # 5

       cluster.shutdown()

Quick Start (CLI — Static Scaling)
-----------------------------------

Scaler has three components that must run together: a **scheduler**, **workers**, and your **client** code.

Start the scheduler with ``scaling=static``, then start a worker manager, then submit work from a client.

**Terminal 1 — Object storage server:**

.. code-block:: bash

   scaler_object_storage_server tcp://127.0.0.1:8517

**Terminal 2 — Scheduler:**

.. code-block:: bash

   scaler_scheduler tcp://127.0.0.1:8516 --object-storage-address tcp://127.0.0.1:8517 \
       --policy-content "allocate=even_load; scaling=static"

.. note::
   The scheduler monitor endpoint defaults to scheduler port + 2 (for example, ``tcp://127.0.0.1:8518``).

**Terminal 3 — Workers:**

.. code-block:: bash

   scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 --worker-manager-id wm-native --max-task-concurrency 4

**Terminal 4 — Client (save as** ``my_client.py`` **and run** ``python my_client.py`` **):**

.. code-block:: python

   from scaler import Client

   def add(a, b):
       return a + b

   with Client(address="tcp://127.0.0.1:8516") as client:
       future = client.submit(add, 2, 3)
       print(future.result())  # 5

Quick Start (CLI — Elastic Scaling)
------------------------------------

With ``scaling=vanilla``, the scheduler's scaling policy starts and stops workers as the load changes.

**Terminal 1 — Object storage server:**

.. code-block:: bash

   scaler_object_storage_server tcp://127.0.0.1:8517

**Terminal 2 — Scheduler:**

.. code-block:: bash

   scaler_scheduler tcp://127.0.0.1:8516 \
       --object-storage-address tcp://127.0.0.1:8517 \
       --policy-content "allocate=even_load; scaling=vanilla"


**Terminal 3 — Baremetal Native Worker Manager:**

.. code-block:: bash

   scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 \
       --max-task-concurrency 4

**Terminal 4 — Client (save as** ``my_client.py`` **and run** ``python my_client.py`` **):**

.. code-block:: python

   from scaler import Client

   def square(x):
       return x * x

   with Client(address="tcp://127.0.0.1:8516") as client:
       futures = client.map(square, range(100))
       print([f.result() for f in futures])

Or use a TOML configuration file:

.. code-block:: bash

   scaler config.toml

.. code-block:: toml
   :caption: config.toml

   [object_storage_server]
   bind_address = "tcp://127.0.0.1:8517"

   [scheduler]
   bind_address = "tcp://127.0.0.1:8516"
   object_storage_address = "tcp://127.0.0.1:8517"

   [[worker_manager]]
   type = "baremetal_native"
   scheduler_address = "tcp://127.0.0.1:8516"
   worker_manager_id = "NAT|default"
   max_task_concurrency = 4
   logging_level = "INFO"
   task_timeout_seconds = 60

How It Works
------------

The worker manager connects to the scheduler and waits for scaling commands. On every heartbeat the scheduler sends a ``setDesiredTaskConcurrency`` command that declares the desired worker count per capability set. The worker manager spawns worker subprocesses to converge toward that target, and replaces a worker that dies on its own.

A worker that exits tells its manager, which replaces it at once. A worker that is killed or hangs sends nothing, and is replaced once it misses heartbeats for ``--unit-timeout-seconds``.

To shed a worker, the manager drains it: the worker takes no new task, the scheduler takes back its queued tasks, and the worker exits once its running task finishes. A worker that has not finished within ``--drain-timeout-seconds`` is terminated, and its task runs again elsewhere.

Ready File
----------

With ``--ready-file``, the manager marks itself in service with a file that holds its pid:

1. On start, the manager removes the file, in case a killed manager left it behind.
2. On the first ``WorkerManagerCommand`` from its parent, the manager writes the file, also for a desired task concurrency of zero.
3. On ``WorkerManagerShutdown``, the file stays while the workers drain.
4. Once the last worker is gone, the manager removes the file, then sends ``WorkerManagerDisconnectNotification`` to its parent.
5. On any other exit, the manager removes the file.

A manager that is told to shut down before its first command never writes the file.

A Kubernetes readiness probe that checks both the file and the pid also reads a manager that died without cleanup as not ready:

.. code:: yaml

    readinessProbe:
      exec:
        command: ["sh", "-c", "kill -0 $(cat /tmp/scaler-ready)"]

Configuration Reference
------------------------

.. note::
   For a full list of shared parameters, see :doc:`common_parameters`.

Baremetal Native Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``scheduler_address`` (positional, required): Address of the scheduler (e.g., ``tcp://127.0.0.1:8516``).
* ``--max-task-concurrency`` (``-mtc``): Maximum number of worker subprocesses. Set to ``-1`` for no limit (default: number of CPUs − 1).
* ``--num-of-workers`` (``-n``): Alias for ``--max-task-concurrency``.
* ``--preload``: Python module path to preload in each worker before it accepts tasks (e.g., ``my_package.preload``).
* ``--ready-file``: Path of a file that holds the pid of the manager while it is in service. It lets a platform that hosts the manager, such as a Kubernetes pod, tell a retired manager from one in service. See `Ready File`_.

Common Parameters
~~~~~~~~~~~~~~~~~

For networking, worker behavior, logging, and event loop options, see :doc:`common_parameters`.
