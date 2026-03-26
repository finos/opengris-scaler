Graph Task
==========

Use graph tasks when work has explicit dependencies.
Scaler accepts a `DAG <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_ and executes nodes in dependency order.

.. literalinclude:: ../../../../examples/graphtask_client.py
   :language: python

What the example does:

* Defines a small dependency graph with constants and function calls.
* Uses :py:func:`~Client.get()` to evaluate graph nodes.
* Requests selected keys and receives a dictionary of computed values.

Choose graph submission when dependency scheduling is part of the problem, not just parallel mapping.
