Simple Engine
=============

``simple`` requires a semicolon-delimited ``policy_content`` string with exactly two keys:

* ``allocate``: ``even_load`` or ``capability``
* ``scaling``: ``no``, ``vanilla``, or ``capability``

Quick Start (copy/paste)
------------------------

The example below starts a scheduler and one native worker manager using ``vanilla`` scaling.

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [object_storage_server]
            bind_address = "tcp://127.0.0.1:8517"

            [scheduler]
            bind_address = "tcp://127.0.0.1:8516"
            object_storage_address = "tcp://127.0.0.1:8517"
            policy_engine_type = "simple"
            policy_content = "allocate=even_load; scaling=vanilla"

            [[worker_manager]]
            type = "baremetal_native"
            scheduler_address = "tcp://127.0.0.1:8516"
            object_storage_address = "tcp://127.0.0.1:8517"
            worker_manager_id = "NAT|default"
            max_task_concurrency = 8

        Run command:

        .. code-block:: bash

            scaler config.toml

    .. group-tab:: command line

        .. code-block:: bash

            scaler_object_storage_server tcp://127.0.0.1:8517 &
            scaler_scheduler tcp://127.0.0.1:8516 \
                --object-storage-address tcp://127.0.0.1:8517 \
                --policy-engine-type simple \
                --policy-content "allocate=even_load; scaling=vanilla" &
            scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 \
                --object-storage-address tcp://127.0.0.1:8517 \
                --worker-manager-id NAT|default \
                --max-task-concurrency 8

Other quick policy strings for ``simple``:

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            # No autoscaling
            policy_engine_type = "simple"
            policy_content = "allocate=even_load; scaling=no"

            # Capability-aware autoscaling (recommended pair)
            policy_engine_type = "simple"
            policy_content = "allocate=capability; scaling=capability"

    .. group-tab:: command line

        .. code-block:: text

            # No autoscaling
            --policy-engine-type simple --policy-content "allocate=even_load; scaling=no"

            # Capability-aware autoscaling (recommended pair)
            --policy-engine-type simple --policy-content "allocate=capability; scaling=capability"

Allocation
----------

The ``allocate`` option controls how tasks are assigned to available workers.

* ``allocate=even_load``

  * Spreads tasks across workers evenly.
  * Best for homogeneous workers where any worker can run any task.
  * Commonly paired with ``scaling=vanilla``.

* ``allocate=capability``

  * Routes tasks to workers whose capabilities match task requirements.
  * Best for heterogeneous clusters (for example CPU-only + GPU workers).
  * Should be paired with ``scaling=capability`` so scale-up requests are also capability-aware.

Capability routing example:

.. literalinclude:: ../../../../examples/task_capabilities.py
   :language: python

Scaling
-------

The ``scaling`` option controls how worker capacity grows or shrinks.

* ``scaling=no``

  * Disables scheduler-driven scaling commands.
  * Use for static capacity or external orchestrators.

* ``scaling=vanilla``

  * General autoscaling for homogeneous clusters.
  * Scale up when ``tasks / workers > 10``.
  * Scale down when ``tasks / workers < 1``.

* ``scaling=capability``

  * Capability-aware autoscaling for heterogeneous clusters.
  * Groups demand by capability and scales per capability group.
  * Scale up when ``tasks / capable_workers > 5``.
  * Scale down when ``tasks / capable_workers < 0.5``.

Notes:

* ``policy_content`` must contain exactly ``allocate`` and ``scaling`` keys.
* Scale-up is capped by each manager heartbeat's ``max_task_concurrency``.
* Pending (booting) workers count toward the cap: if ``connected_workers + pending_workers >= max_task_concurrency``, no additional ``StartWorkers`` request is issued, preventing duplicate launches during the boot window.
* Threshold values are currently fixed in code.
