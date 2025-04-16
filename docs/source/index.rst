.. Scaler documentation master file, created by
   sphinx-quickstart on Wed Feb 15 16:00:47 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Scaler's documentation!
================================================

Scaler is a lightweight distributed computing Python framework that lets you easily distribute tasks across multiple machines or parallelize on a single machine.

Scaler is designed to be a drop-in replacement for Dask requiring minimal code changes. Scaler's design focuses on simplicity, reduced overhead, debuggable errors.

Using Scaler's Graph features for ICAAP Wholesale Loss forecasting, we saw a 3x speedup in execution time as compared to Dask.

Key features include:

    * Graph Tasks - submit tasks with complex interdependence
    * Monitoring Dashboard - monitor utilization of workers and task completion in real time
    * Task Profiling - profile and trace execution of tasks
    * Logging of Distributed Work - send logs back to the Client from workers (under development)


Content
=======

.. toctree::
   :maxdepth: 2

   tutorials/quickstart
   tutorials/features
   tutorials/configuration
   tutorials/applications
