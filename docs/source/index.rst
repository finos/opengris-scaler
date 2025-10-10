.. Scaler documentation master file, created by
   sphinx-quickstart on Wed Feb 15 16:00:47 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Scaler's documentation!
==================================

Scaler is a lightweight distributed computing Python framework that lets you easily distribute tasks across multiple machines or parallelize on a single machine.

Scaler is designed to be a drop-in replacement for Dask requiring minimal code changes. Scaler's design focuses on performance, simplicity, reduced overhead, debuggable errors.

Key features include:

    * Python's ``multiprocessing`` module like API - e.g. ``client.map()`` and ``client.submit()``
    * Graph Tasks - submit DAG tasks with complex interdependence
    * Monitoring Dashboard - monitor utilization of workers and task completion in real time
    * Task Profiling - profile and trace execution of tasks


Content
=======

.. toctree::
   :maxdepth: 2

   tutorials/quickstart
   tutorials/features
   tutorials/configuration
   tutorials/examples
   tutorials/development
