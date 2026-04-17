Waterfall Engine
================

``waterfall_v1`` is a rule-based engine for multi-manager deployments.
You define a CSV-like ``policy_content`` where each line is one manager rule.

Quick Start (copy/paste)
------------------------

This example uses two native worker managers to simulate two tiers.

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [object_storage_server]
            bind_address = "tcp://127.0.0.1:8517"

            [scheduler]
            bind_address = "tcp://127.0.0.1:8516"
            object_storage_address = "tcp://127.0.0.1:8517"
            policy_engine_type = "waterfall_v1"
            policy_content = """
            #  priority, worker_manager_id, max_task_concurrency
                     1,        NAT|local1,                   8
                     2,        NAT|burst1,                  50
            """

            [[worker_manager]]
            type = "baremetal_native"
            scheduler_address = "tcp://127.0.0.1:8516"
            object_storage_address = "tcp://127.0.0.1:8517"
            worker_manager_id = "NAT|local1"
            max_task_concurrency = 8

            [[worker_manager]]
            type = "baremetal_native"
            scheduler_address = "tcp://127.0.0.1:8516"
            object_storage_address = "tcp://127.0.0.1:8517"
            worker_manager_id = "NAT|burst1"
            max_task_concurrency = 50

        Run command:

        .. code-block:: bash

            scaler config.toml

    .. group-tab:: command line

        .. code-block:: bash

            scaler_object_storage_server tcp://127.0.0.1:8517 &
            scaler_scheduler tcp://127.0.0.1:8516 \
                --object-storage-address tcp://127.0.0.1:8517 \
                --policy-engine-type waterfall_v1 \
                --policy-content $'1,NAT|local1,8\n2,NAT|burst1,50' &
            scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 \
                --object-storage-address tcp://127.0.0.1:8517 \
                --worker-manager-id NAT|local1 \
                --max-task-concurrency 8 &
            scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 \
                --object-storage-address tcp://127.0.0.1:8517 \
                --worker-manager-id NAT|burst1 \
                --max-task-concurrency 50

Policy Content (CSV-like)
-------------------------

``policy_content`` is a newline-separated CSV-like list. Each non-empty line is one rule:

.. code-block:: text

    priority,worker_manager_id,max_task_concurrency

Example:

.. code-block:: text

    1,NAT|local1,8
    2,NAT|burst1,50

Rules are evaluated as a priority ladder.

* Scale-up goes from smaller ``priority`` values to larger values.
* Scale-down is the reverse, so higher-priority-number tiers drain first.
* Empty or malformed rule lines fail fast.

priority
--------

``priority`` defines tier order for each manager rule.

* Type: integer.
* Smaller number = higher preference.
* ``1`` is preferred before ``2`` for scale-up decisions.
* For scale-down, higher values are reduced before lower values.

Use ``priority`` to encode cost or latency tiers, for example local capacity first and burst capacity second.

worker_manager_id
-----------------

``worker_manager_id`` binds a rule to a specific connected worker manager.

* Must match the manager heartbeat ID exactly.
* Must be non-empty.
* Must be unique per scheduler.
* Each ID can appear only once in ``policy_content``.

If a manager connects without a matching rule, it is ignored by waterfall scaling.

max_task_concurrency
--------------------

``max_task_concurrency`` defines the rule-level capacity cap for a manager.

* Type: integer.
* The effective cap is ``min(rule.max_task_concurrency, heartbeat.max_task_concurrency)``.
* A lower-priority manager only scales up after all higher-priority managers reach their effective caps.
* A higher-priority manager only scales down after lower-priority managers are drained.

Engine behavior and limits
--------------------------

* Allocation policy is fixed to ``capability``: tasks that declare capabilities are only assigned to workers that
  advertise those capabilities; tasks without capabilities can be assigned to any worker.
* Ratio thresholds are fixed at ``10`` for scale-up and ``1`` for scale-down.
* Threshold values are not runtime-configurable.
