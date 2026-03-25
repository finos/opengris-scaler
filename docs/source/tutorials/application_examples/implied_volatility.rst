Calculate Implied Volatility
============================

This example calculates implied volatility for many market prices using ``client.map()``.
The input is chunked because ``client.map()`` is most useful when task bodies are large or computationally heavy.

.. literalinclude:: ../../../../examples/applications/implied_volatility.py
   :language: python
