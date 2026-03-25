Get Option Close Price Parallelly
=================================

This example gets option closing prices for a specified ticker and start date.
It uses ``client.map()`` to reduce overhead from slower I/O operations.

.. literalinclude:: ../../../../examples/applications/yfinance_historical_price.py
   :language: python
