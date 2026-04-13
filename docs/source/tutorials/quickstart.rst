Quickstart
==========

Quick Installation
------------------

Install `uv <https://docs.astral.sh/uv/getting-started/installation>`_:

.. tabs::

    .. group-tab:: Linux / macOS

        .. code-block:: bash

            curl -LsSf https://astral.sh/uv/install.sh | sh

    .. group-tab:: Windows

        .. code-block:: powershell

            powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.sh | iex"

Then create and activate a virtual environment and install OpenGRIS Scaler:

.. code-block:: bash

    uv venv
    source .venv/bin/activate
    uv pip install 'opengris-scaler[all]'

See :ref:`installation_options` for other install choices and optional dependencies.

Start Services
--------------

.. tabs::

    .. group-tab:: Local Scaler Cluster

        The instructions below start a local scheduler with an elastic native worker
        manager (``baremetal_native``).

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

    .. group-tab:: Remote AWS EC2 Cluster

        Run the scheduler, object storage server, and ORB worker manager on an EC2
        instance. Workers are provisioned automatically in the same VPC.

        .. code-block:: toml
            :caption: config.toml (on the EC2 instance)

            [object_storage_server]
            bind_address = "tcp://0.0.0.0:6789"

            [scheduler]
            bind_address = "tcp://0.0.0.0:6788"
            object_storage_address = "tcp://127.0.0.1:6789"
            advertised_object_storage_address = "tcp://<EC2_PUBLIC_IP>:6789"

            [[worker_manager]]
            type = "orb_aws_ec2"
            scheduler_address = "tcp://127.0.0.1:6788"
            worker_manager_id = "wm-orb"
            worker_scheduler_address = "tcp://<EC2_PRIVATE_IP>:6788"
            object_storage_address = "tcp://<EC2_PRIVATE_IP>:6789"
            python_version = "3.14"
            requirements_txt = """
            opengris-scaler>=1.27.0
            numpy
            """
            instance_type = "t3.medium"
            aws_region = "us-east-1"

        .. code-block:: bash

            scaler config.toml

        See :ref:`orb_aws_ec2_ec2_quick_setup` for full setup instructions.

    .. group-tab:: Remote AWS HPC Batch Cluster

        Run the scheduler and worker manager locally; tasks execute as AWS Batch jobs.

        .. code-block:: toml
            :caption: config.toml

            [object_storage_server]
            bind_address = "tcp://127.0.0.1:2346"

            [scheduler]
            bind_address = "tcp://127.0.0.1:2345"
            object_storage_address = "tcp://127.0.0.1:2346"

            [[worker_manager]]
            type = "aws_hpc"
            scheduler_address = "tcp://127.0.0.1:2345"
            object_storage_address = "tcp://127.0.0.1:2346"
            worker_manager_id = "wm-batch"
            job_queue = "scaler-batch-queue"
            job_definition = "scaler-batch-job"
            s3_bucket = "scaler-batch-<account-id>-us-east-1"
            aws_region = "us-east-1"

        .. code-block:: bash

            scaler config.toml

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
