.. rst-class:: hidden-page-title

Examples
========

.. list-table::
   :header-rows: 1

   * - Examples
     - Parfun
     - Pargraph
     - Client
     - Workers
     - Num Workers
     - Ratio: Speed/Workers
     - Sequential Runtime
     - Parallel Runtime
     - Speedup
   * - :doc:`Multi-Signal Alpha Research <../gallery/AlphaResearch>`
     - Yes
     - No
     - AWS
     - EC2
     - 8
     - 0.31
     - 14m 38s
     - 5m 49s
     - 2.51
   * - :doc:`Vol Surface Calibration & PDE Exotic Pricing <../gallery/VolSurface>`
     - Yes
     - No
     - AWS
     - EC2
     - 128
     - 0.26
     - 81m 46s
     - 2m 12s
     - 33
   * - :doc:`Swap Portfolio CVA <../gallery/SwapCVA>`
     - Yes
     - Yes
     - AWS
     - EC2
     - 64
     - 0.43
     - 35m 12s
     - 1m 16s
     - 27.6
   * - :doc:`Portfolio-Level XVA Risk <../gallery/XVA>`
     - No
     - Yes
     - NATIVE
     - NATIVE
     - 16
     - 0.64
     - 64m 04s
     - 6m 14s
     - 10.27

.. toctree::
   :hidden:
   :maxdepth: 1
   :titlesonly:

   Multi-Signal Alpha Research <../gallery/AlphaResearch>
   Vol Surface Calibration & PDE Exotic Pricing <../gallery/VolSurface>
   Swap Portfolio CVA <../gallery/SwapCVA>
   Portfolio-Level XVA Risk <../gallery/XVA>
