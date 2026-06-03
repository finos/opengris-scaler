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
