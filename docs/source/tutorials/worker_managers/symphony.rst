IBM Spectrum Symphony Worker Manager
=====================================

The Symphony worker manager integrates Scaler with `IBM Spectrum Symphony <https://www.ibm.com/products/analytics-workload-management>`_, allowing Scaler to offload task execution to a Symphony cluster via the SOAM (Service-Oriented Architecture Middleware) API.

Quick Start
-----------

Prerequisites
~~~~~~~~~~~~~

* An IBM Spectrum Symphony cluster with a configured service
* The ``soamapi`` Python package installed (``pip install soamapi``)
* Python packages: ``pip install opengris-scaler``
* Network connectivity between the machine running the worker manager and both the Scaler scheduler and the Symphony cluster

Step 1: Install Dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   pip install opengris-scaler soamapi

Step 2: Start the Scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   scaler_scheduler tcp://0.0.0.0:8516 \
       --policy-content "allocate=even_load; scaling=vanilla"

.. note::
   The default scaling policy is ``scaling=no`` (no auto-scaling). The ``scaling=vanilla`` policy is required for
   the worker manager to dynamically provision workers.

Step 3: Start the Symphony Worker Manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   scaler_worker_manager_symphony tcp://<SCHEDULER_IP>:8516 \
       --service-name MyScalerService \
       --max-task-concurrency 8

Or use a TOML configuration file:

.. code-block:: bash

   scaler_worker_manager_symphony tcp://<SCHEDULER_IP>:8516 --config config.toml

.. code-block:: toml
   :caption: config.toml

   [symphony_worker_manager]
   service_name = "MyScalerService"
   max_task_concurrency = 8
   logging_level = "INFO"

Step 4: Submit Tasks
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from scaler import Client

   def compute(x):
       return x ** 2

   with Client(address="tcp://<SCHEDULER_IP>:8516") as client:
       futures = client.map(compute, range(50))
       results = [f.result() for f in futures]
       print(results)

How It Works
------------

1. The Symphony worker manager connects to the Scaler scheduler as a worker.
2. It establishes a SOAM connection and session to the configured Symphony service.
3. When the worker manager receives a task from the scheduler, it serializes the function and arguments with ``cloudpickle`` and submits them as a Symphony task via the SOAM API.
4. Symphony schedules the task on its compute hosts. On completion, the SOAM callback delivers the result back to the worker manager.
5. The worker manager deserializes the result and returns it to the Scaler scheduler.

The worker manager uses a concurrency semaphore to limit the number of tasks in flight.

Configuration Reference
------------------------

Symphony-Specific Parameters
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``scheduler_address`` (positional, required): Address of the Scaler scheduler.
* ``--service-name`` (``-sn``, required): The name of the Symphony service to connect to.
* ``--max-task-concurrency`` (``-mtc``): Maximum number of concurrent Symphony workers (default: number of CPUs − 1).

Common Parameters
~~~~~~~~~~~~~~~~~

For networking, worker behavior, logging, and event loop options, see :doc:`common_parameters`.
