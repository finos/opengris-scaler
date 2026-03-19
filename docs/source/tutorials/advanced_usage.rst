Advanced Usage
==============

Simple Client
-------------

Shows how to send a basic task to scheduler.

.. literalinclude:: ../../../examples/simple_client.py
   :language: python

Client Mapping Tasks
--------------------

Shows how to use ``client.map()``.

.. literalinclude:: ../../../examples/map_client.py
   :language: python

Graph Task
----------

Shows how to send a graph-based task to scheduler.

.. literalinclude:: ../../../examples/graphtask_client.py
   :language: python

Nested Task (Submit A Parallel Task Within A Parallel Task)
-----------------------------------------------------------

Shows how to send a nested task to scheduler.

.. literalinclude:: ../../../examples/nested_client.py
   :language: python

Nested Graph Task (Submit Graph Tasks Recursively)
--------------------------------------------------

Shows how to dynamically build graph in the remote end.

.. warning::
   This is a toy example; it is not recommended to build recursion that deep, as it will be slow.

.. literalinclude:: ../../../examples/graphtask_nested_client.py
   :language: python

Disconnect Client
-----------------

Shows how to disconnect a client from scheduler.

.. literalinclude:: ../../../examples/disconnect_client.py
   :language: python

Capability Allocation Example
-----------------------------

Shows how to use capabilities for task routing.

.. literalinclude:: ../../../examples/task_capabilities.py
   :language: python
