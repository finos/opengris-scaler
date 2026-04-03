Baremetal Native Worker Manager
===============================

The Baremetal Native worker manager spawns worker subprocesses on the local machine. It is the simplest way to run Scaler across multiple CPU cores and is the recommended starting point for most users.

It supports two modes:

* **Dynamic mode** (``scaler_worker_manager baremetal_native``): Workers are provisioned and destroyed on demand by the scheduler's scaling policy.
* **Fixed mode** (``scaler_worker_manager baremetal_native --mode fixed``): A static pool of workers is spawned at startup. No dynamic scaling.

Quick Start (Python API)
------------------------

The fastest way to get going is with ``SchedulerClusterCombo``, which starts a scheduler, object storage server, and a fixed pool of workers in a single Python process:

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

Quick Start (CLI — Fixed Workers)
----------------------------------

Scaler has three components that must run together: a **scheduler**, **workers**, and your **client** code.

Start the scheduler first, then start a fixed pool of workers, then submit work from a client.

**Terminal 1 — Object storage server:**

.. code-block:: bash

   scaler_object_storage_server tcp://127.0.0.1:8517

**Terminal 2 — Scheduler:**

.. code-block:: bash

   scaler_scheduler tcp://127.0.0.1:8516 --object-storage-address tcp://127.0.0.1:8517

.. note::
   The scheduler monitor endpoint defaults to scheduler port + 2 (for example, ``tcp://127.0.0.1:8518``).

**Terminal 3 — Workers:**

.. code-block:: bash

   scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 --mode fixed --max-task-concurrency 4

**Terminal 4 — Client (save as** ``my_client.py`` **and run** ``python my_client.py`` **):**

.. code-block:: python

   from scaler import Client

   def add(a, b):
       return a + b

   with Client(address="tcp://127.0.0.1:8516") as client:
       future = client.submit(add, 2, 3)
       print(future.result())  # 5

Quick Start (CLI — Dynamic Scaling)
------------------------------------

For dynamic scaling, use ``scaler_worker_manager baremetal_native`` (without ``--mode fixed``). The scheduler's scaling policy will automatically start and stop workers as needed.

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

Quick Start (CLI — Fixed Mode)
-------------------------------

You can also use ``scaler_worker_manager baremetal_native`` in fixed mode, which spawns a static pool of workers at startup.

**Terminal 1 — Object storage server:**

.. code-block:: bash

   scaler_object_storage_server tcp://127.0.0.1:8517

**Terminal 2 — Scheduler:**

.. code-block:: bash

   scaler_scheduler tcp://127.0.0.1:8516 --object-storage-address tcp://127.0.0.1:8517

**Terminal 3 — Baremetal Native Worker Manager (Fixed):**

.. code-block:: bash

   scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 \
       --mode fixed \
       --max-task-concurrency 8

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
   worker_manager_id = "NAT|fixed"
   mode = "fixed"
   max_task_concurrency = 8
   logging_level = "INFO"

How It Works
------------

**Dynamic mode:** The worker manager connects to the scheduler and waits for scaling commands. When the scheduler's scaling policy determines that more workers are needed, it sends a ``StartWorkerGroup`` command. The worker manager spawns a new worker subprocess. When the scheduler wants to scale down, it sends a ``ShutdownWorkerGroup`` command and the worker manager terminates the worker. Each worker group contains exactly one worker process.

**Fixed mode** (``scaler_worker_manager baremetal_native --mode fixed``): A fixed number of worker subprocesses are spawned immediately at startup and connect to the scheduler. Workers are not dynamically scaled. If a worker terminates, it is **not** automatically restarted.

Configuration Reference
------------------------

.. note::
   For a full list of shared parameters, see :doc:`common_parameters`.

Baremetal Native Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``scheduler_address`` (positional, required): Address of the scheduler (e.g., ``tcp://127.0.0.1:8516``).
* ``--max-task-concurrency`` (``-mtc``): Maximum number of worker subprocesses. In dynamic mode, set to ``-1`` for no limit (default: number of CPUs − 1). In fixed mode, this is the exact number of workers spawned.
* ``--mode``: Operating mode: ``dynamic`` (default) for auto-scaling driven by scheduler, or ``fixed`` for pre-spawned workers.
* ``--num-of-workers`` (``-n``): Alias for ``--max-task-concurrency``.
* ``--preload``: Python module path to preload in each worker before it accepts tasks (e.g., ``my_package.preload``).

Common Parameters
~~~~~~~~~~~~~~~~~

For networking, worker behavior, logging, and event loop options, see :doc:`common_parameters`.
