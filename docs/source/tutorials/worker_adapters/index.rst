Worker Adapters
===============

Worker Adapters are components in Scaler that handle the actual provisioning and destruction of worker resources. They act as the bridge between Scaler's scaling policies and various infrastructure providers (e.g., local processes, cloud instances, container orchestrators).

.. note::
    For more details on how to configure Scaler, see the :doc:`../configuration` section.

.. note::
    By default, the scheduler starts with the ``no`` scaling policy, meaning no workers are provisioned automatically. A worker adapter will connect to the scheduler but will not receive any scaling commands until a policy that performs scaling is configured. To enable auto-scaling, pass a ``--policy-content`` (``-pc``) flag to the scheduler.

Enabling Auto-Scaling
---------------------

To enable auto-scaling with a worker adapter, configure the scheduler's scaling policy using the ``--policy-content`` (``-pc``) flag. For example, to use the vanilla scaler:

.. code-block:: bash

    scaler_scheduler tcp://127.0.0.1:8516 -pc "allocate=even_load; scaling=vanilla"

Once the scheduler is running with this policy, start a worker adapter (e.g., the Native adapter):

.. code-block:: bash

    scaler_worker_adapter_native tcp://127.0.0.1:8516 --max-workers 8

The vanilla policy will then automatically scale workers up and down based on the task-to-worker ratio. For a full description of available scaling policies and their parameters, see :doc:`../scaling`.

Adapters Overview
-----------------

Scaler provides several worker adapters to support different execution environments.

Native
~~~~~~

The :doc:`Native <native>` worker adapter allows Scaler to dynamically provision workers as local subprocesses on the same machine. It is the simplest way to scale workloads across multiple CPU cores locally and supports dynamic auto-scaling.

Fixed Native
~~~~~~~~~~~~

The :doc:`Fixed Native <fixed_native>` worker adapter spawns a static number of worker subprocesses at startup and does not support dynamic scaling. It is the underlying component used by the high-level ``Cluster`` and ``SchedulerClusterCombo`` classes.

AWS HPC
~~~~~~~

The :doc:`AWS HPC <aws_hpc/index>` worker adapter allows Scaler to offload task execution to cloud environments, currently supporting AWS Batch. It is ideal for bursting workloads to the cloud or utilizing specific hardware not available locally.

Common Parameters
~~~~~~~~~~~~~~~~~

All worker adapters share a set of :doc:`common configuration parameters <common_parameters>` for networking, worker behavior, and logging.

.. toctree::
    :hidden:

    native
    fixed_native
    aws_hpc/index
    common_parameters
