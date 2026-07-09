Quickstart
==========

.. image:: images/quickstart_steps.svg
   :alt: Quickstart steps overview

Interested? Try it out yourself, step by step.

.. admonition:: Try the client in your browser
   :class: tip

   Prefer to skip the install for now? You can run a handful of small demo
   notebooks directly from the browser via JupyterLite -- see :doc:`try_in_browser`.

Quick Installation
------------------

Install `uv <https://docs.astral.sh/uv/getting-started/installation>`_:

.. tabs::

    .. group-tab:: Linux / macOS

        .. code-block:: bash

            curl -LsSf https://astral.sh/uv/install.sh | sh

    .. group-tab:: Windows

        .. code-block:: powershell

            powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

Restart your shell. Then create and activate a virtual environment and install OpenGRIS Scaler:

.. tabs::

    .. group-tab:: Linux / macOS

        .. code-block:: bash

            uv venv
            source .venv/bin/activate
            uv pip install 'opengris-scaler[all]'

    .. group-tab:: Windows

        .. code-block:: powershell

            uv venv
            .venv\Scripts\activate
            uv pip install 'opengris-scaler[all]'

See :ref:`installation_options` for other install choices and optional dependencies.

Start Services
--------------

.. tabs::

    .. group-tab:: Remote AWS EC2

        Run the scheduler, object storage server, and ORB worker manager on an EC2
        instance. Workers are provisioned automatically in the same VPC.

        See :ref:`orb_aws_ec2_ec2_quick_setup` for full setup instructions.

    .. group-tab:: Remote AWS HPC Batch

        Run the scheduler and worker manager locally; tasks execute as AWS Batch jobs.

        See :ref:`aws_hpc_batch_quick_start` for full setup instructions.

.. _quickstart_start_compute_tasks:

Start Compute Tasks
-------------------

While Scaler :doc:`provides a lower level API for submitting tasks <scaler_client>`,
it's most often used through its associated higher-level APIs:
`parfun <https://github.com/finos/opengris-parfun>`_ and
`pargraph <https://github.com/finos/opengris-pargraph>`_.

Make sure you get these libraries installed before running the examples below:

.. code-block:: bash

    uv pip install numpy scipy pandas opengris-parfun pargraph

Basic examples

- :doc:`Task parallelism using parfun <../gallery/parfun>`
- :doc:`Concurrent graph computation using pargraph <../gallery/pargraph>`

Additional Jupyter notebooks demonstrating real-world distributed computing use cases with Scaler, parfun, and pargraph:

- :doc:`Multi-Signal Alpha Research Platform with Parfun <../gallery/AlphaResearch>`
- :doc:`Parallel Vol Surface Calibration & PDE Exotic Pricing with Parfun <../gallery/VolSurface>`
- :doc:`Parallel Swap Portfolio CVA with Pargraph + Parfun <../gallery/SwapCVA>`
- :doc:`Portfolio-Level XVA Risk Computation with Pargraph <../gallery/XVA>`

These notebooks need a few extra libraries and Jupyter. See
:doc:`examples` for step-by-step setup instructions, or install everything at
once with ``uv pip install -r examples/notebooks/requirements_notebooks.txt``.
