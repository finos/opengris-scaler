Examples
========

This page shows some real life examples. Before reading this page, be sure that you have read examples in the
Quickstart section. Those examples are better starting point than examples in this page.

Basic Usage
-----------

Simple Client
~~~~~~~~~~~~~

Shows how to send a basic task to scheduler

.. literalinclude:: ../../../examples/simple_client.py
   :language: python

Client Mapping Tasks
~~~~~~~~~~~~~~~~~~~~

Shows how to use ``client.map()``

.. literalinclude:: ../../../examples/map_client.py
   :language: python

Graph Task
~~~~~~~~~~

Shows how to send a graph based task to scheduler

.. literalinclude:: ../../../examples/graphtask_client.py
   :language: python

Nested Task (Submit A Parallel Task Within A Parallel Task)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Shows how to send a nested task to scheduler

.. literalinclude:: ../../../examples/nested_client.py
   :language: python

Nested Graph Task (Submit Graph Tasks Recursively)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Shows how to dynamically build graph in the remote end

.. warning::
   This is a toy example, it's not recommended to build recursion that deep, as it will be slow.

.. literalinclude:: ../../../examples/graphtask_nested_client.py
   :language: python

Disconnect Client
~~~~~~~~~~~~~~~~~

Shows how to disconnect a client from scheduler

.. literalinclude:: ../../../examples/disconnect_client.py
   :language: python

Capability Allocation Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Shows how to use capabilities for task routing

.. literalinclude:: ../../../examples/task_capabilities.py
   :language: python

Applications
------------

Calculate Implied Volatility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example calculate implied volatility for many given market price. We use `client.map` to achieve such goal.
Notice that we provide chunk of data as input to `find_volatilities`. This is because `client.map` makes sense only
in two cases: A. The body of the function you are submitting is quite large; B. The function is very computation
heavy or is possible to block.

.. literalinclude:: ../../../examples/applications/implied_volatility.py
   :language: python

Get Option Close Price Parallelly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example gets option closing price for a specified ticker and the start date. We use `client.map` to reduce the
overhead introduced by slow IO speed.

.. literalinclude:: ../../../examples/applications/yfinance_historical_price.py
   :language: python

Distributed Image Processing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example uses the Pillow library with Scaler to resize images in parallel.

.. literalinclude:: ../../../examples/applications/pillow.py
   :language: python

Parallel Timeseries Cross-Validation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example uses the Prophet library with Scaler to perform parallelized cross-validation.

.. literalinclude:: ../../../examples/applications/timeseries.py
   :language: python
