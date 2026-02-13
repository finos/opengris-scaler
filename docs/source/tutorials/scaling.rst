Scaling Policies
================

Scaler provides an *experimental* auto-scaling feature that allows the system to dynamically adjust the number of workers based on workload. Scaling policies determine when to add or remove workers, while Worker Adapters handle the actual provisioning of resources.

Overview
--------

The scaling system consists of two main components:

1. **Scaling Controller**: A policy that monitors task queues and worker availability to make scaling decisions.
2. **Worker Adapter**: A component that handles the actual creation and destruction of worker groups (e.g., starting containers, launching processes).

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
        --policy-content "allocate=even_load; scaling=vanilla" \
        --adapter-webhook-urls "http://localhost:8080/webhook"


Capability Scaling (``capability``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The capability scaling controller is designed for heterogeneous workloads where tasks require specific capabilities (e.g., GPU, high memory, specialized hardware).

**Key Features:**

* Groups tasks by their required capability sets
* Groups workers by their provided capability sets
* Scales worker groups per capability set independently
* Ensures tasks are matched to workers that can handle them
* Prevents scaling down the last worker group capable of handling pending tasks

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
        --policy-content "allocate=capability; scaling=capability" \
        --adapter-webhook-urls "http://localhost:8080/webhook"

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


**Worker Adapter Integration:**

The capability scaling controller communicates with the worker adapter via HTTP webhooks. When requesting a new worker group, it includes the required capabilities:

.. code:: json

    {
        "action": "start_worker_group",
        "capabilities": {"gpu": 1}
    }

The worker adapter should provision workers with the requested capabilities and return:

.. code:: json

    {
        "worker_group_id": "group-abc123",
        "worker_ids": ["worker-1", "worker-2"],
        "capabilities": {"gpu": 1}
    }


Fixed Elastic Scaling (``fixed_elastic``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The fixed elastic scaling controller supports hybrid scaling with two worker adapters:

* **Primary Adapter**: Limited number of worker groups (e.g., on-premise resources)
* **Secondary Adapter**: Overflow capacity (e.g., cloud burst)

This is useful for scenarios where you have a fixed pool of dedicated resources but want to burst to cloud resources during peak demand.

.. code:: bash

    scaler_scheduler tcp://127.0.0.1:8516 \
        --policy-content "allocate=even_load; scaling=fixed_elastic" \
        --adapter-webhook-urls "http://localhost:8080/primary" "http://localhost:8081/secondary"

**Behavior:**

* New worker groups are created from the primary adapter until its limit is reached
* Once primary is at capacity, new groups are created from the secondary adapter
* When scaling down, secondary adapter groups are shut down first


Worker Adapter Protocol
-----------------------

Scaling controllers communicate with worker adapters via HTTP POST requests to a webhook URL. The adapter must implement the following actions:

**Get Adapter Info:**

Request:

.. code:: json

    {"action": "get_worker_adapter_info"}

Response:

.. code:: json

    {
        "max_worker_groups": 10
    }

**Start Worker Group:**

Request:

.. code:: json

    {
        "action": "start_worker_group",
        "capabilities": {"gpu": 1}
    }

Response (success - HTTP 200):

.. code:: json

    {
        "worker_group_id": "group-abc123",
        "worker_ids": ["worker-1", "worker-2"],
        "capabilities": {"gpu": 1}
    }

Response (capacity exceeded - HTTP 429):

.. code:: json

    {"error": "Capacity exceeded"}

**Shutdown Worker Group:**

Request:

.. code:: json

    {
        "action": "shutdown_worker_group",
        "worker_group_id": "group-abc123"
    }

Response (success - HTTP 200):

.. code:: json

    {"status": "shutdown"}

Response (not found - HTTP 404):

.. code:: json

    {"error": "Worker group not found"}


Example Worker Adapter
----------------------

Here is an example of a simple worker adapter using the ECS (Amazon Elastic Container Service) integration:

.. literalinclude:: ../../../src/scaler/worker_adapter/ecs.py
   :language: python


Tips
----

1. **Match allocation and scaling policies**: Use ``allocate=capability`` with ``scaling=capability`` to ensure tasks are routed to workers with the right capabilities.

2. **Set appropriate thresholds**: Adjust task ratio thresholds based on your task duration and scaling latency:

   * Short tasks: Use higher ``upper_task_ratio`` to avoid thrashing
   * Long startup time: Use higher ``lower_task_ratio`` to avoid premature scale-down

3. **Monitor scaling events**: Use Scaler's monitoring tools (``scaler_top``) to observe scaling behavior and tune policies.

