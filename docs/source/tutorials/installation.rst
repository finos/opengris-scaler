.. _installation_options:

Installation
============

The ``opengris-scaler`` package is available on PyPI and can be installed using any compatible package manager. The examples below use `uv <https://docs.astral.sh/uv/getting-started/installation>`_.

Base installation:

.. code-block:: bash

    uv pip install opengris-scaler

If you need the web GUI:

.. code-block:: bash

    uv pip install opengris-scaler[gui]

If you use GraphBLAS to solve DAG graph tasks:

.. code-block:: bash

    uv pip install opengris-scaler[graphblas]

If you need all optional dependencies:

.. code-block:: bash

    uv pip install opengris-scaler[all]
