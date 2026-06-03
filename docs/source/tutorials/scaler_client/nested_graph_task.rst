Nested Graph Task (Submit Graph Tasks Recursively)
==================================================

Use this pattern when the execution graph is only known at runtime and must be built on workers.

.. warning::
   This is a toy example; it is not recommended to build recursion that deep, as it will be slow.

.. literalinclude:: ../../../../examples/graphtask_nested_client.py
   :language: python

What the example does:

* Submits a recursive Fibonacci task.
* Builds a small graph inside each recursive call to compute ``n-1`` and ``n-2``.
* Uses :py:func:`~Client.get()` for graph evaluation and :py:func:`~Client.submit()` for recursion.

This demonstrates dynamic graph construction on workers. Prefer flatter task designs for production workloads.
