Scaling Policies
================

Scaler's auto-scaling is controlled by the scheduler policy engine. Pick the engine with
``policy_engine_type``, then configure it with ``policy_content``.

Available policy engines:

.. list-table::
   :header-rows: 1
   :widths: 18 28 54

   * - Policy Engine
     - Description
     - Best For
   * - :doc:`simple <scaling/simple>`
     - General-purpose engine for most deployments.
     -
       * One manager or a small manager set
       * Homogeneous workloads: ``even_load + vanilla``
       * Capability-aware workloads: ``capability + capability``
       * Baseline + burst: ``fixed_elastic``
   * - :doc:`waterfall_v1 <scaling/waterfall_v1>`
     - Priority-based multi-manager scaling.
     -
       * Strict tier preference across managers
       * Cost tiers (local/on-prem first, cloud overflow second)
       * Latency tiers where preferred resources fill first
       * Deterministic scale-up and scale-down order

.. toctree::
   :maxdepth: 1
   :hidden:

   scaling/simple
   scaling/waterfall_v1
