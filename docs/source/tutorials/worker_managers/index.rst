Worker Managers
===============

Worker managers handle the provisioning and destruction of worker resources. They bridge Scaler's
:doc:`scaling policies <../scaling>` and the underlying infrastructure — local processes, cloud
instances, or container orchestrators.

.. note::
    For more details on Scaler configuration, see the :doc:`../configuration` section.

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

Although worker managers target different infrastructures, many configuration options are shared.
See :doc:`Common Worker Manager Parameters <common_parameters>` for these shared settings.

.. toctree::
    :maxdepth: 1
    :hidden:

    baremetal_native
    aws_hpc_batch
    aws_raw_ecs
    symphony
    common_parameters
