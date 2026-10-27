Capability Allocation Example
=============================

Shows how to use capabilities for task routing.

.. literalinclude:: ../../../../examples/task_capabilities.py
   :language: python

What the example does:

* Starts a scheduler with capability-aware allocation policy.
* Adds a worker manager that advertises ``gpu`` capability.
* Submits one task with ``capabilities={"gpu": 1}`` and another with no capability requirement.

Why this matters:

* Capability constraints route tasks to compatible workers.
* Non-constrained tasks can use general workers, improving cluster utilization.

.. _client-capabilities:

Client capabilities
-------------------

Pass ``capabilities`` to ``Client`` to apply them to every task it submits.
A submission's own ``capabilities``, ``{}`` included, replaces the client's.
A client serialized into a task keeps its capabilities.

.. code:: python

    with Client(address="tcp://127.0.0.1:2345", capabilities={"gpu": 1}) as client:
        client.submit(gpu_task, 16.0)  # requires gpu
        client.submit_verbose(cpu_task, args=(16.0,), kwargs={}, capabilities={})  # requires nothing
