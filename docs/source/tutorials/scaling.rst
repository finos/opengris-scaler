Scaling Policies
================

Scaler provides an *experimental* auto-scaling feature that allows the system to dynamically adjust the number of workers based on workload. Scaling policies determine when to add or remove workers, while Worker Adapters handle the actual provisioning of resources.

Overview
--------

The scaling system consists of two main components:

1. **Scaling Controller**: A policy that monitors task queues and worker availability to make scaling decisions.
2. **Worker Adapter**: A component that handles the actual creation and destruction of worker groups (e.g., starting containers, launching processes).

The Scaling Controller runs within the Scheduler and communicates with Worker Adapters via Cap'n Proto messages. Worker Adapters connect to the Scheduler and receive scaling commands directly.

The scaling policy is configured via the ``policy_content`` setting in the scheduler configuration:

.. code:: bash

    scaler_scheduler tcp://127.0.0.1:8516 --policy-content "allocate=capability; scaling=vanilla"

Or in a TOML configuration file:

.. code:: toml

    [scheduler]
    policy_content = "allocate=capability; scaling=capability"

Available Scaling Policies
--------------------------

Scaler provides several built-in scaling policies:

.. list-table:: Scaling Policies
   :widths: 20 80
   :header-rows: 1

   * - Policy
     - Description
   * - ``no``
     - No automatic scaling. Workers are managed manually or statically provisioned.
   * - ``vanilla``
     - Basic task-to-worker ratio scaling. Scales up when task ratio exceeds threshold, scales down when idle.
   * - ``capability``
     - Capability-aware scaling. Scales worker groups based on task-required capabilities (e.g., GPU, memory).
   * - ``fixed_elastic``
     - Hybrid scaling using primary and secondary worker adapters with configurable limits.


No Scaling (``no``)
~~~~~~~~~~~~~~~~~~~

The simplest policy that performs no automatic scaling. Use this when:

* Workers are statically provisioned
* External orchestration handles scaling (e.g., Kubernetes HPA)
* You want full manual control over worker count

.. code:: bash

    scaler_scheduler tcp://127.0.0.1:8516 --policy-content "allocate=even_load; scaling=no"


Vanilla Scaling (``vanilla``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The vanilla scaling controller uses a simple task-to-worker ratio to make scaling decisions:

* **Scale up**: When ``tasks / workers > upper_task_ratio`` (default: 10)
* **Scale down**: When ``tasks / workers < lower_task_ratio`` (default: 1)

This policy is straightforward and works well for homogeneous workloads where all workers can handle all tasks.

.. code:: bash

    scaler_scheduler tcp://127.0.0.1:8516 \
        --policy-content "allocate=even_load; scaling=vanilla"


Capability Scaling (``capability``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The capability scaling controller is designed for heterogeneous workloads where tasks require specific capabilities (e.g., GPU, high memory, specialized hardware).

**Key Features:**

* Groups tasks by their required capability sets
* Groups workers by their provided capability sets
* Scales worker groups per capability set independently
* Ensures tasks are matched to workers that can handle them
* Prevents scaling down the last worker group capable of handling pending tasks
* Prevents thrashing by checking if scale-down would immediately trigger scale-up

**How It Works:**

1. **Task Grouping**: Tasks are grouped by their required capability keys (e.g., ``{"gpu"}``, ``{"gpu", "high_memory"}``).

2. **Worker Matching**: Workers are grouped by their provided capabilities. A worker can handle a task if the task's required capabilities are a subset of the worker's capabilities.

3. **Per-Capability Scaling**: The controller applies the task-to-worker ratio logic independently for each capability set:

   * **Scale up**: When ``tasks / capable_workers > upper_task_ratio`` (default: 5)
   * **Scale down**: When ``tasks / capable_workers < lower_task_ratio`` (default: 0.5)

4. **Capability Request**: When scaling up, the controller requests worker groups with specific capabilities from the worker adapter.

**Configuration:**

.. code:: bash

    scaler_scheduler tcp://127.0.0.1:8516 \
        --policy-content "allocate=capability; scaling=capability"

**Example Scenario:**

Consider a workload with both CPU-only and GPU tasks:

.. code:: python

    from scaler import Client

    with Client(address="tcp://127.0.0.1:8516") as client:
        # Submit CPU tasks (no special capabilities required)
        cpu_futures = [
            client.submit_verbose(cpu_task, args=(i,), capabilities={})
            for i in range(100)
        ]

        # Submit GPU tasks (require GPU capability)
        gpu_futures = [
            client.submit_verbose(gpu_task, args=(i,), capabilities={"gpu": 1})
            for i in range(50)
        ]

With the capability scaling policy:

1. If no GPU workers exist, the controller requests a worker group with ``{"gpu": 1}`` from the adapter.
2. CPU and GPU worker groups are scaled independently based on their respective task queues.
3. Idle GPU workers can be shut down without affecting CPU task processing.


Fixed Elastic Scaling (``fixed_elastic``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The fixed elastic scaling controller supports hybrid scaling with multiple worker adapters:

* **Primary Adapter**: A single worker group (identified by ``max_worker_groups == 1``) that starts once and never shuts down
* **Secondary Adapter**: Elastic capacity (``max_worker_groups > 1``) that scales based on demand

This is useful for scenarios where you have a fixed pool of dedicated resources but want to burst to additional resources during peak demand.

.. code:: bash

    scaler_scheduler tcp://127.0.0.1:8516 \
        --policy-content "allocate=even_load; scaling=fixed_elastic"

**Behavior:**

* The primary adapter's worker group is started once and never shut down
* Secondary adapter groups are created when demand exceeds primary capacity
* When scaling down, only secondary adapter groups are shut down


Worker Adapter Protocol
-----------------------

Scaling controllers, running within the scheduler process, communicate with worker adapters using Cap'n Proto messages through the connection that worker adapters use to communicate with the scheduler. The protocol uses the following message types:

**WorkerAdapterHeartbeat (Adapter -> Scheduler):**

Worker adapters periodically send heartbeats to the scheduler containing their capacity information:

* ``max_worker_groups``: Maximum number of worker groups this adapter can manage
* ``workers_per_group``: Number of workers in each group
* ``capabilities``: Default capabilities for workers from this adapter

**WorkerAdapterCommand (Scheduler -> Adapter):**

The scheduler sends commands to worker adapters:

* ``StartWorkerGroup``: Request to start a new worker group

  * ``worker_group_id``: Empty for new groups (adapter assigns ID)
  * ``capabilities``: Required capabilities for the worker group

* ``ShutdownWorkerGroup``: Request to shut down an existing worker group

  * ``worker_group_id``: ID of the group to shut down

**WorkerAdapterCommandResponse (Adapter -> Scheduler):**

Worker adapters respond to commands with status and details:

* ``worker_group_id``: ID of the affected worker group
* ``command``: The command type this response is for
* ``status``: Result status (``Success``, ``WorkerGroupTooMuch``, ``WorkerGroupIDNotFound``)
* ``worker_ids``: List of worker IDs in the group (for start commands)
* ``capabilities``: Actual capabilities of the started workers


Example Worker Adapter
----------------------

Here is an example of a worker adapter using the ECS (Amazon Elastic Container Service) integration:

.. literalinclude:: ../../../src/scaler/worker_manager_adapter/aws_raw/ecs.py
   :language: python


Tips
----

1. **Match allocation and scaling policies**: Use ``allocate=capability`` with ``scaling=capability`` to ensure tasks are routed to workers with the right capabilities.

2. **Set appropriate thresholds**: Adjust task ratio thresholds based on your task duration and scaling latency:

   * Short tasks: Use higher ``upper_task_ratio`` to avoid thrashing
   * Long startup time: Use higher ``lower_task_ratio`` to avoid premature scale-down

3. **Monitor scaling events**: Use Scaler's monitoring tools (``scaler_top``) to observe scaling behavior and tune policies.

4. **Worker Adapter Placement**: Run worker adapters on machines that can provision the required resources (e.g., run the ECS adapter where it has AWS credentials, run the native adapter on the target machine).
