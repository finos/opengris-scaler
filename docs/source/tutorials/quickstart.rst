Quickstart
==========

Quick Installation
------------------

.. code:: bash

    pip install opengris-scaler[all]

See :ref:`installation_options` for other install choices and optional dependencies.

Start Services
--------------

The instructions below start a local scheduler with an elastic native worker
manager (``baremetal_native``). If you want to run scheduler + worker manager
locally but provision workers on cloud infrastructure, use these quick starts:

* :ref:`Open Resource Broker AWS EC2 Quick Start (EC2) <orb_aws_ec2_ec2_quick_setup>`.
* :ref:`AWS HPC Batch Quick Start <aws_hpc_batch_quick_start>`.

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [object_storage_server]
            bind_address = "tcp://127.0.0.1:8517"

            [scheduler]
            bind_address = "tcp://127.0.0.1:8516"
            object_storage_address = "tcp://127.0.0.1:8517"

            [[worker_manager]]
            type = "baremetal_native"
            scheduler_address = "tcp://127.0.0.1:8516"
            worker_manager_id = "wm-native"

        Run command:

        .. code-block:: bash

            scaler config.toml

    .. group-tab:: command line

        In terminal 1:

        .. code-block:: bash

            scaler_object_storage_server tcp://127.0.0.1:8517

        In terminal 2:

        .. code-block:: bash

            scaler_scheduler tcp://127.0.0.1:8516 --object-storage-address tcp://127.0.0.1:8517

        In terminal 3:

        .. code-block:: bash

            scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 --worker-manager-id wm-native


Start Compute Tasks
-------------------

Connect the Python client and submit tasks:

.. code:: python

    from scaler import Client


    def square(value):
        return value * value


    with Client(address="tcp://127.0.0.1:8516") as client:
        results = client.map(square, range(0, 100))

    print(results)
