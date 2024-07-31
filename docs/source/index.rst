.. scaler documentation master file, created by
   sphinx-quickstart on Wed Feb 15 16:00:47 2023.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to scaler's documentation!
================================================

Scaler is a lightweight distributed computing Python framework. It allocates work over multiple workers (distributedly or locally through multiprocessing). The motivation behind Scaler was the difficulty of debugging Dask jobs that were hanging. It is meant to be a drop-in replacement for Dask Futures with improvements and additional features.

Scaler's design focuses on simplicity, resulting in reduced overhead and debuggable errors. Based on our benchmarking results for ICAAP Wholesale Loss forecasting, it took Scaler Graph only a third of the time to finish compared to Dask.

Lastly, Scaler's load-balancers work in a more predictable manner, increasing utilization of workers. The best part is that Scaler can be adopted in place of Dask with minimal code change!

Key features include:

    * Graph Task Submission - submit task with dependencies together
    * Monitoring Dashboard - utilization of workers and task completion can be seen
    * Task Profiling - seeing execution time of functions
    * Logging of Distributed Work - send logs back to the Client from workers (under development)


Content
=======

.. toctree::
   :maxdepth: 2

   tutorials/quickstart
   tutorials/features
   tutorials/configuration
