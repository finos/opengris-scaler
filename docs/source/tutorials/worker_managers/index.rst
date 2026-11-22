Worker Managers
===============

Worker managers handle the provisioning and destruction of worker resources. They bridge Scaler's
:doc:`policy engine <../policy_engine>` and the underlying infrastructure — local processes, cloud
instances, or container orchestrators.

.. note::
    For more details on Scaler configuration, see :doc:`../commands`.

.. note::
    By default, the scheduler uses ``allocate=even_load; scaling=vanilla``, which scales workers with the load. To choose another policy, pass ``--policy-content`` (``-pc``) to the scheduler.

Enabling Auto-Scaling
---------------------

Configure the scheduler with a scaling policy, then start a worker manager:

.. code-block:: bash

    # Terminal 1 — Object storage server
    scaler_object_storage_server tcp://127.0.0.1:8517

    # Terminal 2 — Scheduler
    scaler_scheduler tcp://127.0.0.1:8516 --object-storage-address tcp://127.0.0.1:8517 -pc "allocate=even_load; scaling=vanilla"

    # Terminal 3 — Worker Manager (e.g., Baremetal Native)
    scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 --max-task-concurrency 8

The vanilla policy automatically scales workers up and down based on the task-to-worker ratio. For available policies and their parameters, see :doc:`../policy_engine`.

Worker Managers Overview
------------------------


.. list-table::
   :header-rows: 1
   :widths: 20 40 20 20

   * - Worker Manager
     - Description
     - Scaling
     - Infrastructure
   * - :doc:`ORB AWS EC2 <orb_aws_ec2/index>`
     - Dynamically provisions workers on AWS EC2 instances using the ORB system.
     - Dynamic (scheduler-driven)
     - AWS EC2
   * - :doc:`AWS HPC Batch <aws_hpc_batch>`
     - Runs each task as an AWS Batch job on managed EC2 compute.
     - Concurrency-limited
     - AWS Batch + S3
   * - :doc:`AWS Raw ECS <aws_raw_ecs>`
     - Provisions full Scaler worker processes as Fargate tasks.
     - Dynamic (scheduler-driven)
     - AWS ECS Fargate
   * - :doc:`Baremetal Native <baremetal_native>`
     - Spawns workers as local subprocesses. The simplest worker manager and the recommended starting point.
     - Dynamic (scheduler-driven)
     - Local machine
   * - :doc:`Symphony <symphony>`
     - Offloads tasks to IBM Spectrum Symphony via the SOAM API.
     - Concurrency-limited
     - IBM Symphony
   * - :doc:`OCI Raw <oci_raw>`
     - Provisions full Scaler worker processes as OCI Container Instances.
     - Dynamic (scheduler-driven)
     - OCI Container Instances
   * - :doc:`OCI HPC <oci_hpc>`
     - Runs each task as a dedicated OCI Container Instance job.
     - Concurrency-limited
     - OCI Container Instances + Object Storage

Although worker managers target different infrastructures, many configuration options are shared.
See :doc:`Common Worker Manager Parameters <common_parameters>` for these shared settings.

Unified Entry Points
~~~~~~~~~~~~~~~~~~~~

The :ref:`scaler_worker_manager <cmd-scaler-worker-manager>` command provides a single entry point for all worker managers.
The :ref:`scaler <cmd-scaler>` command boots the full stack from a single TOML config file.

Demo Videos
-----------

.. raw:: html

   <p>Waterfall</p>
   <video controls preload="metadata" style="width: 100%; max-width: 1000px;" src="https://github.com/user-attachments/assets/45556417-7497-47db-b0f4-c2b4f07662fc"></video>
   <p>Kubernetes (in development, not part of this release)</p>
   <video controls preload="metadata" style="width: 100%; max-width: 1000px;" src="https://github.com/user-attachments/assets/a973f03b-f11c-42de-8da3-576e5081d36c"></video>
   <p>OCI</p>
   <video controls preload="metadata" style="width: 100%; max-width: 1000px;" src="https://github.com/user-attachments/assets/1e405ae1-29db-4208-b454-1002fd1dcdd6"></video>

.. toctree::
    :maxdepth: 1
    :hidden:

    orb_aws_ec2/index
    aws_hpc_batch
    aws_raw_ecs
    baremetal_native
    symphony
    oci_raw
    oci_hpc
    common_parameters
