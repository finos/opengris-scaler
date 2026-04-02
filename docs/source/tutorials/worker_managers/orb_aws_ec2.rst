Open Resource Broker AWS EC2 Worker Manager
===========================================

Use this worker manager to provision Scaler workers dynamically on AWS EC2 via
ORB (Open Resource Broker).

Requirements
------------

Before using the ORB AWS EC2 worker manager, make sure:

* You have an AWS account.
* Python is installed on the machine that runs the worker manager.
* The scheduler host can be reached from provisioned AWS workers. If your scheduler is behind a firewall/private network, set up NAT so workers can connect back to the scheduler.

.. _orb_aws_ec2_quick_setup:

Quick Setup
-----------

.. warning::

   Do not use ``pip install awscli`` for this setup. That installs AWS CLI v1.
   Use the official AWS CLI v2 installer instead.

.. tabs::

   .. group-tab:: Linux x86_64

      .. code-block:: bash

         curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
         unzip awscliv2.zip
         sudo ./aws/install

         aws --version

   .. group-tab:: Linux ARM64

      .. code-block:: bash

         curl "https://awscli.amazonaws.com/awscli-exe-linux-aarch64.zip" -o "awscliv2.zip"
         unzip awscliv2.zip
         sudo ./aws/install

Then create a new virtual environment and install Scaler with ORB extras:

.. code-block:: bash

   python -m venv .venv
   source .venv/bin/activate
   pip install opengris-scaler[orb]

Then authenticate with AWS CLI:

.. tabs::

   .. group-tab:: Local Machine

      .. code-block:: bash

         aws login

      Click the page link and proceed in your default browser to sign in, then
      follow the AWS CLI instructions in the terminal.

   .. group-tab:: Remote Server

      .. code-block:: bash

         aws login --remote

      Open the URL printed by the command in your local browser, complete
      sign-in, then copy the returned code/token and paste it back into the
      remote terminal to finish login.

AWS Permissions
~~~~~~~~~~~~~~~

.. tabs::

   .. group-tab:: AWS Root account

      The AWS root account does not require additional IAM policy grants for this setup.

   .. group-tab:: IAM User

      If you are using the root account, first create an IAM user. Before running
      the commands below, prepare these values:

      * IAM user name (for ``--user-name``)
      * AWS account ID (for policy ARN)
      * IAM role name, if attaching the same policy to a role

      Then create the policy and attach it to an IAM user/role:

      .. code-block:: bash

         aws iam create-policy \
           --policy-name ScalerORBWorkerManagerPolicy \
           --policy-document '{
             "Version": "2012-10-17",
             "Statement": [
               {
                 "Effect": "Allow",
                 "Action": [
                   "ec2:CancelSpotFleetRequests",
                   "ec2:CreateFleet",
                   "ec2:CreateKeyPair",
                   "ec2:CreateLaunchTemplate",
                   "ec2:CreateSecurityGroup",
                   "ec2:CreateTags",
                   "ec2:DeleteFleet",
                   "ec2:DeleteKeyPair",
                   "ec2:DeleteLaunchTemplate",
                   "ec2:DeleteNetworkInterface",
                   "ec2:DeleteSecurityGroup",
                   "ec2:DeleteVolume",
                   "ec2:DescribeFleets",
                   "ec2:DescribeImages",
                   "ec2:DescribeInstanceStatus",
                   "ec2:DescribeInstances",
                   "ec2:DescribeInstanceTypes",
                   "ec2:DescribeLaunchTemplates",
                   "ec2:DescribeNetworkInterfaces",
                   "ec2:DescribeSecurityGroups",
                   "ec2:DescribeSpotFleetInstances",
                   "ec2:DescribeSpotFleetRequests",
                   "ec2:DescribeSubnets",
                   "ec2:DescribeVolumes",
                   "ec2:DescribeVpcs",
                   "ec2:RequestSpotFleet",
                   "ec2:RunInstances",
                   "ec2:TerminateInstances",
                   "autoscaling:CreateAutoScalingGroup",
                   "autoscaling:CreateLaunchConfiguration",
                   "autoscaling:CreateOrUpdateTags",
                   "autoscaling:DeleteAutoScalingGroup",
                   "autoscaling:DeleteLaunchConfiguration",
                   "autoscaling:DescribeAutoScalingGroups",
                   "autoscaling:DescribeAutoScalingInstances",
                   "autoscaling:UpdateAutoScalingGroup",
                   "iam:GetRole",
                   "iam:PassRole",
                   "ssm:GetParameter",
                   "sts:GetCallerIdentity"
                 ],
                 "Resource": "*"
               }
             ]
           }'

      Attach to an IAM user:

      .. code-block:: bash

         aws iam attach-user-policy \
           --user-name <YOUR_USER> \
           --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/ScalerORBWorkerManagerPolicy

      Attach to an IAM role:

      .. code-block:: bash

         aws iam attach-role-policy \
           --role-name <YOUR_ROLE> \
           --policy-arn arn:aws:iam::<ACCOUNT_ID>:policy/ScalerORBWorkerManagerPolicy

      Spot Fleet service-linked role (once per account, only if using Spot Fleet):

      .. code-block:: bash

         aws iam create-service-linked-role --aws-service-name spotfleet.amazonaws.com

      Get your account ID:

      .. code-block:: bash

         aws sts get-caller-identity --query Account --output text

Start Services
~~~~~~~~~~~~~~

Before starting services, make sure NAT setup is complete. If this machine
already has a public IP, you can ignore NAT setup. Then copy the
``config.toml`` below and replace ``PUBLIC_IP`` with your real public IP
address.

.. tabs::

   .. group-tab:: config.toml

      .. code-block:: toml

         [scheduler]
         # use 0.0.0.0 so NAT and forward traffic from your public IP to this machine
         scheduler_address = "tcp://0.0.0.0:8516"

         [[worker_manager]]
         type = "orb_aws_ec2"
         scheduler_address = "tcp://127.0.0.1:8516"   
         worker_manager_id = "wm-orb"
         # worker provisioned in AWS need reach to your PUBLIC_IP, and your router 
         # then forward packets to the machine you started services
         object_storage_address = "tcp://<PUBLIC_IP>:8517"
         public_scheduler_address = "tcp://<PUBLIC_IP>:8516"
         # You can start either with pre-built AMI or with specified python version
         # and requirements_txt
         # image_id = "ami-..."
         python_version = "3.13"
         requirements_txt = """
         opengris-scaler>=1.27.0
         numpy
         pandas
         """
         instance_type = "t3.medium"
         aws_region = "us-east-1"
         logging_level = "INFO"
         task_timeout_seconds = 60

      Run command:

      .. code-block:: bash

         scaler config.toml

   .. group-tab:: command line

      .. code-block:: bash

         scaler_scheduler tcp://0.0.0.0:8516
         scaler_worker_manager orb_aws_ec2 tcp://127.0.0.1:8516 \
             --worker-manager-id wm-orb \
             --public-scheduler-address tcp://<PUBLIC_IP>:8516 \
             --object-storage-address tcp://<PUBLIC_IP>:8517 \
             --python-version 3.13 \
             --requirements-txt /path/to/requirements.txt \
             --instance-type t3.medium \
             --aws-region us-east-1 \
             --logging-level INFO \
             --task-timeout-seconds 60

After services are up, you should be good to go and can use the client to
submit tasks to workers provisioned on AWS.

Worker Image Customization Modes
--------------------------------

The adapter supports two mutually exclusive worker-image modes. Choose exactly one:

* Use an existing pre-built AMI image.
* Use a base image and install Python/packages when instances start.

**Base Image + Startup Install**

Provide both ``--python-version`` and ``--requirements-txt`` (both required). Instances use the
base Amazon Linux 2023 (AL2023) image, then install the specified Python version and dependencies
at startup. ``opengris-scaler`` must be included in ``requirements_txt``.

.. tabs::

   .. group-tab:: config.toml

      .. code-block:: toml

         [[worker_manager]]
         type = "orb_aws_ec2"
         scheduler_address = "tcp://<SCHEDULER_IP>:8516"
         worker_manager_id = "wm-orb"
         public_scheduler_address = "tcp://<PUBLIC_IP>:8516"
         object_storage_address = "tcp://<PUBLIC_IP>:8517"
         instance_type = "t3.medium"
         python_version = "3.13"
         requirements_txt = """
         opengris-scaler>=1.26.6
         numpy
         pandas
         """

      Run command:

      .. code-block:: bash

         scaler config.toml

   .. group-tab:: command line

      .. code-block:: bash

         # Requirements as a file path
         scaler_worker_manager orb_aws_ec2 tcp://<SCHEDULER_IP>:8516 \
             --worker-manager-id wm-orb \
             --public-scheduler-address tcp://<PUBLIC_IP>:8516 \
             --object-storage-address tcp://<PUBLIC_IP>:8517 \
             --instance-type t3.medium \
             --python-version 3.13 \
             --requirements-txt /path/to/requirements.txt

         # Requirements as a string literal
         scaler_worker_manager orb_aws_ec2 tcp://<SCHEDULER_IP>:8516 \
             --worker-manager-id wm-orb \
             --public-scheduler-address tcp://<PUBLIC_IP>:8516 \
             --object-storage-address tcp://<PUBLIC_IP>:8517 \
             --instance-type t3.medium \
             --python-version 3.13 \
             --requirements-txt "opengris-scaler>=1.26.6"

**Existing Pre-built AMI**

Provide ``--image-id``. The specified AMI is used as-is and must already include
``opengris-scaler`` with ``scaler_worker_manager`` available on ``PATH``.

This mode is recommended for production deployments where startup latency matters or where the
worker environment must be tightly controlled.

.. tabs::

   .. group-tab:: config.toml

      .. code-block:: toml

         [[worker_manager]]
         type = "orb_aws_ec2"
         scheduler_address = "tcp://<SCHEDULER_IP>:8516"
         worker_manager_id = "wm-orb"
         public_scheduler_address = "tcp://<PUBLIC_IP>:8516"
         object_storage_address = "tcp://<PUBLIC_IP>:8517"
         instance_type = "t3.medium"
         image_id = "ami-0123456789abcdef0"

      Run command:

      .. code-block:: bash

         scaler config.toml

   .. group-tab:: command line

      .. code-block:: bash

         scaler_worker_manager orb_aws_ec2 tcp://<SCHEDULER_IP>:8516 \
             --worker-manager-id wm-orb \
             --public-scheduler-address tcp://<PUBLIC_IP>:8516 \
             --object-storage-address tcp://<PUBLIC_IP>:8517 \
             --instance-type t3.medium \
             --image-id ami-0123456789abcdef0

Networking Configuration
------------------------

Workers launched by the ORB AWS EC2 manager are EC2 instances and require an externally-reachable IP address for the scheduler.

*   **Internal Communication**: If the machine running the scheduler is another EC2 instance in the same VPC, you can use EC2 private IP addresses.
*   **Public Internet**: If communicating over the public internet, it is highly recommended to set up robust security rules and/or a VPN to protect the cluster.

Supported Parameters
--------------------

.. note::
    For more details on how to configure Scaler, see the :doc:`../commands` section.

The ORB AWS EC2 worker manager supports ORB-specific configuration parameters as well as common worker manager parameters.

ORB AWS EC2 Template Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*   ``--image-id``: AMI ID for the worker instances. Mutually exclusive with ``--python-version`` and
    ``--requirements-txt``. When provided, the latest AL2023 AMI is not used and no packages are installed.
*   ``--python-version``: Python version to install on each worker instance (e.g. ``3.13``). Required when
    ``--image-id`` is not specified.
*   ``--requirements-txt``: Requirements to install on each worker instance. Can be a path to a local
    ``requirements.txt`` file or a string literal. The content is embedded in the EC2 user data script and
    installed via ``pip install -r``. ``opengris-scaler`` must be included. Required when ``--image-id`` is
    not specified.
*   ``--instance-type``: EC2 instance type (default: ``t2.micro``).
*   ``--aws-region``: AWS region (default: ``us-east-1``).
*   ``--key-name``: AWS key pair name for the instances. If not provided, a temporary key pair will be created and deleted on cleanup.
*   ``--subnet-id``: AWS subnet ID where the instances will be launched. If not provided, it attempts to discover the default subnet in the default VPC.
*   ``--security-group-ids``: Comma-separated list of AWS security group IDs.

Common Parameters
~~~~~~~~~~~~~~~~~

For a full list of common parameters including networking (``--worker-manager-id``, ``--max-task-concurrency``, ``--object-storage-address``, etc.), worker configuration, and logging, see :doc:`common_parameters`.

Cleanup
-------

The ORB AWS EC2 worker manager is designed to be self-cleaning, but it is important to be aware of the resources it manages:

*   **Key Pairs**: If a ``--key-name`` is not provided, the manager creates a temporary AWS key pair.
*   **Security Groups**: If ``--security-group-ids`` are not provided, the manager creates a temporary security group to allow communication.
*   **Launch Templates**: ORB may additionally create EC2 Launch Templates as part of the machine provisioning process.

The manager attempts to delete these temporary resources and terminate all launched EC2 instances when it shuts down gracefully. However, in the event of an ungraceful crash or network failure, some resources may persist in your AWS account.

.. tip::
    It is recommended to periodically check your AWS console for any orphaned resources (instances, security groups, key pairs, or launch templates) and clean them up manually if necessary to avoid unexpected costs.
