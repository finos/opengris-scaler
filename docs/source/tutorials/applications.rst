


Applications
============

This page shows some real life examples. Before reading this page, be sure that you have read examples in the 
Quickstart section. Those examples are better starting point than examples in this page. 


Calculate Implied Volatility
----------------------------

This example calculate implied volatility for many given market price. We use `client.map` to achieve such goal.
Notice that we provide chunk of data as input to `find_volatilities`. This is because `client.map` makes sense only 
in two cases: A. The body of the function you are submitting is quite large; B. The function is very computation 
heavy or is possible to block.

.. literalinclude:: ../../../examples/applications/implied_volatility.py
   :language: python


Get Option Closing Price
------------------------

This example gets option closing price for a specified ticker and the start date. We use `client.map` to reduce the
overhead introduced by slow IO speed.

.. literalinclude:: ../../../examples/applications/yfinance_historical_price.py
   :language: python



