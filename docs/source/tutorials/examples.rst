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

Running these notebooks locally
-------------------------------

The notebooks live under ``examples/notebooks/`` in the
`OpenGRIS Scaler repository <https://github.com/finos/opengris-scaler>`_. From a
fresh checkout, set them up in four steps:

1. **Set up your environment.** Create and activate a virtual environment, then
   install OpenGRIS Scaler (see :ref:`installation_options` for more options):

   .. code-block:: bash

       uv venv
       source .venv/bin/activate
       uv pip install opengris-scaler

2. **Install the notebook dependencies** from the bundled requirements file:

   .. code-block:: bash

       uv pip install -r examples/notebooks/requirements_notebooks.txt

3. **Install Jupyter:**

   .. code-block:: bash

       uv pip install jupyter

4. **Launch Jupyter and run the code:**

   .. code-block:: bash

       cd examples/notebooks
       jupyter notebook

   Open a notebook and run its cells in order. Notebooks that connect to a
   Scaler cluster over a ``tcp://`` address expect a
   :doc:`running scheduler and cluster <../tutorials/quickstart>`; the others
   start a local cluster automatically.

.. toctree::
   :hidden:
   :maxdepth: 1
   :titlesonly:

   Multi-Signal Alpha Research <../gallery/AlphaResearch>
   Vol Surface Calibration & PDE Exotic Pricing <../gallery/VolSurface>
   Swap Portfolio CVA <../gallery/SwapCVA>
   Portfolio-Level XVA Risk <../gallery/XVA>
