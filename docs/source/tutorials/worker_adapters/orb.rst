ORB Worker Adapter
==================

The ORB worker adapter allows Scaler to dynamically provision workers on AWS EC2 instances using the ORB (Open Resource Broker) system. This is particularly useful for scaling workloads that require significant compute resources or specialized hardware available in the cloud.

This tutorial describes the steps required to get up and running with the ORB adapter.

Requirements
------------

Before using the ORB worker adapter, ensure the following requirements are met on the machine that will run the adapter:

1.  **orb-py and boto3**: The ``orb-py`` and ``boto3`` packages must be installed. These can be installed using the ``orb`` optional dependency of Scaler:

    .. code-block:: bash

        pip install "opengris-scaler[orb]"

2.  **AWS CLI**: The AWS Command Line Interface must be installed and configured with a default profile that has permissions to launch, describe, and terminate EC2 instances.

3.  **Network Connectivity**: The adapter must be able to communicate with AWS APIs and the Scaler scheduler.

Getting Started
---------------

To start the ORB worker adapter, use the ``scaler_worker_adapter_orb`` command.

Example command:

.. code-block:: bash

    scaler_worker_adapter_orb tcp://<SCHEDULER_EXTERNAL_IP>:8516 \
        --object-storage-address tcp://<OSS_EXTERNAL_IP>:8517 \
        --adapter-web-host 0.0.0.0 \
        --adapter-web-port 8080 \
        --image-id ami-0528819f94f4f5fa5 \
        --instance-type t3.medium \
        --aws-region us-east-1 \
        --logging-level INFO \
        --task-timeout-seconds 60

Equivalent configuration using a TOML file:

.. code-block:: bash

    scaler_worker_adapter_orb tcp://<SCHEDULER_EXTERNAL_IP>:8516 --config config.toml

.. code-block:: toml

    # config.toml

    [orb_worker_adapter]
    object_storage_address = "tcp://<OSS_EXTERNAL_IP>:8517"
    adapter_web_host = "0.0.0.0"
    adapter_web_port = 8080
    image_id = "ami-0528819f94f4f5fa5"
    instance_type = "t3.medium"
    aws_region = "us-east-1"
    logging_level = "INFO"
    task_timeout_seconds = 60

*   ``tcp://<SCHEDULER_EXTERNAL_IP>:8516`` is the address workers will use to connect to the scheduler.
*   ``tcp://<OSS_EXTERNAL_IP>:8517`` is the address workers will use to connect to the object storage server.
*   The adapter HTTP server (webhook) will listen on port 8080. The scheduler connects to this.
*   New workers will be launched using the specified AMI and instance type.

Networking Configuration
------------------------

Workers launched by the ORB adapter are EC2 instances and require an externally-reachable IP address for the scheduler.

*   **Internal Communication**: If the machine running the scheduler is another EC2 instance in the same VPC, you can use EC2 private IP addresses.
*   **Public Internet**: If communicating over the public internet, it is highly recommended to set up robust security rules and/or a VPN to protect the cluster.

Publicly Available AMIs
-----------------------

We regularly publish publicly available Amazon Machine Images (AMIs) with Python and ``opengris-scaler`` pre-installed.

.. list-table:: Available Public AMIs
   :widths: 15 15 20 20 30
   :header-rows: 1

   * - Scaler Version
     - Python Version
     - Amazon Linux 2023 Version
     - Date (MM/DD/YYYY)
     - AMI ID (us-east-1)
   * - 1.14.2
     - 3.13
     - 2023.10.20260120
     - 01/30/2026
     - ``ami-0528819f94f4f5fa5``

New AMIs will be added to this list as they become available.

Supported Parameters
--------------------

.. note::
    For more details on how to configure Scaler, see the :doc:`../configuration` section.

The ORB worker adapter supports ORB-specific configuration parameters as well as common worker adapter parameters.

Orb Template Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~

*   ``--image-id`` (Required): AMI ID for the worker instances.
*   ``--instance-type``: EC2 instance type (default: ``t2.micro``).
*   ``--aws-region``: AWS region (default: ``us-east-1``).
*   ``--key-name``: AWS key pair name for the instances. If not provided, a temporary key pair will be created and deleted on cleanup.
*   ``--subnet-id``: AWS subnet ID where the instances will be launched. If not provided, it attempts to discover the default subnet in the default VPC.
*   ``--security-group-ids``: Comma-separated list of AWS security group IDs.
*   ``--allowed-ip``: IP address to allow in the security group (if created automatically). Defaults to the adapter's external IP.
*   ``--orb-config-path``: Path to the directory containing the ORB configuration files in `config/` (default: ``src/scaler/worker_adapter/drivers/orb_common``).

Common Parameters
~~~~~~~~~~~~~~~~~

For a full list of common parameters including networking, worker configuration, and logging, see :doc:`common_parameters`.

Cleanup
-------

The ORB worker adapter is designed to be self-cleaning, but it is important to be aware of the resources it manages:

*   **Key Pairs**: If a ``--key-name`` is not provided, the adapter creates a temporary AWS key pair.
*   **Security Groups**: If ``--security-group-ids`` are not provided, the adapter creates a temporary security group to allow communication.
*   **Launch Templates**: ORB may additionally create EC2 Launch Templates as part of the machine provisioning process.

The adapter attempts to delete these temporary resources and terminate all launched EC2 instances when it shuts down gracefully. However, in the event of an ungraceful crash or network failure, some resources may persist in your AWS account.

.. tip::
    It is recommended to periodically check your AWS console for any orphaned resources (instances, security groups, key pairs, or launch templates) and clean them up manually if necessary to avoid unexpected costs.

.. warning::
    **Subnet and Security Groups**: Currently, specifying ``--subnet-id`` or ``--security-group-ids`` via configuration might not have the intended effect as the adapter is designed to auto-discover or create these resources. Specifically, the adapter may still attempt to use default subnets or create its own temporary security groups regardless of these parameters.
