Worker Managers
===============

Worker managers handle the provisioning and destruction of worker resources. They bridge Scaler's scaling policies and the underlying infrastructure — local processes, cloud instances, or container orchestrators.

.. note::
    For more details on Scaler configuration, see the :doc:`../configuration` section.

.. note::
    By default, the scheduler starts with the ``no`` scaling policy, meaning no workers are provisioned automatically. To enable auto-scaling, pass ``--policy-content`` (``-pc``) to the scheduler.

Enabling Auto-Scaling
---------------------

Configure the scheduler with a scaling policy, then start a worker manager:

.. code-block:: bash

    # Terminal 1 — Scheduler
    scaler_scheduler tcp://127.0.0.1:8516 -pc "allocate=even_load; scaling=vanilla"

    # Terminal 2 — Worker Manager (e.g., Baremetal Native)
    scaler_worker_manager_baremetal_native tcp://127.0.0.1:8516 --max-task-concurrency 8

The vanilla policy automatically scales workers up and down based on the task-to-worker ratio. For available policies and their parameters, see :doc:`../scaling`.

Worker Managers Overview
-----------------------

.. list-table::
   :header-rows: 1
   :widths: 20 40 20 20

   * - Worker Manager
     - Description
     - Scaling
     - Infrastructure
   * - :doc:`Baremetal Native <baremetal_native>`
     - Spawns workers as local subprocesses. The simplest worker manager and the recommended starting point.
     - Dynamic or fixed
     - Local machine
   * - :doc:`AWS HPC Batch <aws_hpc_batch>`
     - Runs each task as an AWS Batch job on managed EC2 compute.
     - Concurrency-limited
     - AWS Batch + S3
   * - :doc:`AWS Raw ECS <aws_raw_ecs>`
     - Provisions full Scaler worker processes as Fargate tasks.
     - Dynamic (scheduler-driven)
     - AWS ECS Fargate
   * - :doc:`Symphony <symphony>`
     - Offloads tasks to IBM Spectrum Symphony via the SOAM API.
     - Concurrency-limited
     - IBM Symphony

Common Parameters
~~~~~~~~~~~~~~~~~

All worker managers share a set of :doc:`common configuration parameters <common_parameters>` for networking, worker behavior, and logging.

.. toctree::
    :hidden:

    baremetal_native
    aws_hpc_batch
    aws_raw_ecs
    symphony
    common_parameters
