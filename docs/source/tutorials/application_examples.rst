Application Examples
====================

This page shows real-world application examples.

Calculate Implied Volatility
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example calculates implied volatility for many market prices using ``client.map()``.
The input is chunked because ``client.map()`` is most useful when task bodies are large or computationally heavy.

.. literalinclude:: ../../../examples/applications/implied_volatility.py
   :language: python

Get Option Close Price Parallelly
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example gets option closing prices for a specified ticker and start date.
It uses ``client.map()`` to reduce overhead from slower I/O operations.

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
