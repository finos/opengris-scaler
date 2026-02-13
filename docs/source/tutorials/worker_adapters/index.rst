Worker Adapters
===============

Worker Adapters are components in Scaler that handle the actual provisioning and destruction of worker resources. They act as the bridge between Scaler's scaling policies and various infrastructure providers (e.g., local processes, cloud instances, container orchestrators).

.. note::
    For more details on how to configure Scaler, see the :doc:`../configuration` section.

.. toctree::
   :maxdepth: 1

   native
   fixed_native
   orb
   common_parameters

Adapter Overviews
-----------------

Scaler provides several worker adapters to support different execution environments.

Native
~~~~~~

The :doc:`Native <native>` worker adapter allows Scaler to dynamically provision workers as local subprocesses on the same machine. It is the simplest way to scale workloads across multiple CPU cores locally and supports dynamic auto-scaling.

Fixed Native
~~~~~~~~~~~~

The :doc:`Fixed Native <fixed_native>` worker adapter spawns a static number of worker subprocesses at startup and does not support dynamic scaling. It is the underlying component used by the high-level ``Cluster`` and ``SchedulerClusterCombo`` classes.

ORB (AWS EC2)
~~~~~~~~~~~~~

The :doc:`ORB <orb>` worker adapter allows Scaler to dynamically provision workers on AWS EC2 instances. This is ideal for scaling workloads that require significant cloud compute resources or specialized hardware like GPUs.

Common Parameters
~~~~~~~~~~~~~~~~~

All worker adapters share a set of :doc:`common configuration parameters <common_parameters>` for networking, worker behavior, and logging.
