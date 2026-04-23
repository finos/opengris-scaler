Overview
========

Architecture
------------

Below is a diagram of the relationship between multiple Clients, the Scheduler,
Worker Managers, and Workers.

.. image:: images/architecture.svg
   :alt: Scaler architecture diagram
   :width: 1000px


* Multiple clients can submit tasks to the same scheduler concurrently.
* The client is responsible for serializing tasks and arguments.
* Multiple worker managers can connect to the same scheduler and provision capacity in parallel.
* Worker managers spawn workers, and workers connect directly to the scheduler.
* The scheduler dispatches tasks to connected workers, and workers execute tasks and return results.

Key Features
------------

* Cross cloud computing support with unified and single client api
* Easy spawn clusters on either local machine or clouds
* Python ``multiprocessing``-style client API, for example ``client.map()`` ``client.starmap()`` and ``client.submit()``.
* Graph tasks for DAG-based execution with explicit dependencies use ``client.get()``.
* Both CLI and WebUI Monitoring dashboard for real-time worker and task visibility.
* Task profiling for runtime and resource diagnostics.


For code API examples and client patterns, see :doc:`scaler_client`.

Start Services
--------------

Scaler supports multiple service startup patterns. Choose the one that matches
your deployment style:

* **Local bundled services in Python (fixed workers):** use ``SchedulerClusterCombo``
  to start object storage, scheduler, and a fixed-size local worker pool in one
  Python flow. See :doc:`worker_managers/baremetal_native`.

* **Local elastic workers managed by a worker manager:** run scheduler +
  ``baremetal_native`` worker manager in dynamic mode and let scheduler policies
  scale workers up/down. See :doc:`quickstart`, :doc:`worker_managers/index`,
  and :doc:`policy_engine`.

* **Remote elastic workers managed by cloud worker managers:** keep the scheduler
  on one machine and attach one or more remote managers (for example,
  ``aws_raw_ecs``, ``aws_hpc``, ``orb_aws_ec2``, ``symphony``). See
  :doc:`worker_managers/index`.

* **Fully separated services:** start object storage server, scheduler, and one
  or more worker managers as independent processes (possibly on different
  machines). See :ref:`cmd-scaler-object-storage-server`,
  :ref:`cmd-scaler-scheduler`, and :ref:`cmd-scaler-worker-manager`.

* **Single TOML launcher:** use :ref:`cmd-scaler` to start scheduler and one or
  more worker managers from one configuration file.
