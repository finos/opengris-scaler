AWS Raw ECS Worker Manager
==========================

The AWS Raw ECS worker manager provisions Scaler workers as `AWS Fargate <https://aws.amazon.com/fargate/>`_ tasks inside an `ECS <https://aws.amazon.com/ecs/>`_ cluster. Unlike the :doc:`AWS HPC Batch worker manager <aws_hpc_batch>`, which runs each Scaler *task* as a separate cloud job, the AWS Raw ECS worker manager launches full Scaler *worker processes* in Fargate containers. This means workers connect back to the scheduler and process tasks the same way local workers do, with the scheduler handling load balancing and scaling.

Prerequisites
-------------

* An AWS account
* `AWS CLI <https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html>`_ installed and configured (``aws configure``)
* Python packages: ``pip install opengris-scaler boto3``
* A VPC with at least one subnet that has internet access (a public subnet with an Internet Gateway, or a private subnet with a NAT Gateway)

Quick Start
-----------

Get a subnet ID from your default VPC:

.. code-block:: bash

   aws ec2 describe-subnets \
       --filters "Name=default-for-az,Values=true" \
       --query "Subnets[0].SubnetId" \
       --output text

Paste the result into the TOML below and run the three commands:

.. code-block:: toml
   :caption: config.toml

   [[worker_manager]]
   type = "aws_raw_ecs"
   scheduler_address = "tcp://<SCHEDULER_PUBLIC_IP>:8516"
   object_storage_address = "tcp://<SCHEDULER_PUBLIC_IP>:8517"
   worker_manager_id = "wm-ecs"
   ecs_subnets = "subnet-0abc1234def56789a"  # paste your subnet ID here
   aws_region = "us-east-1"
   max_task_concurrency = 4
   ecs_task_cpu = 4
   ecs_task_memory = 30

.. code-block:: bash

   # Terminal 1 — Scheduler (use your public/private IP, not 127.0.0.1)
   scaler_scheduler tcp://0.0.0.0:8516 \
       --policy-content "allocate=even_load; scaling=vanilla"


.. code-block:: bash

   # Terminal 2 — AWS Raw ECS Worker Manager
   $ scaler config.toml

.. code-block:: python
   :caption: my_client.py (Terminal 3)

   from scaler import Client

   def compute(x):
       return x ** 2

   with Client(address="tcp://<SCHEDULER_PUBLIC_IP>:8516") as client:
       futures = client.map(compute, range(50))
       print([f.result() for f in futures])

If you need help finding your subnet IDs or setting up permissions, follow the detailed setup below.

Detailed Setup
--------------

Step 1: Configure AWS Credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   aws configure
   # Enter your AWS Access Key ID, Secret Access Key, region (e.g. us-east-1), and output format (json)

Your IAM user needs the following permissions:

* **ECS**: ``ecs:CreateCluster``, ``ecs:DescribeClusters``, ``ecs:RegisterTaskDefinition``, ``ecs:DescribeTaskDefinition``, ``ecs:RunTask``, ``ecs:StopTask``
* **IAM**: ``iam:CreateRole``, ``iam:AttachRolePolicy``, ``iam:GetRole``, ``iam:PassRole``
* **EC2**: ``ec2:DescribeSubnets``, ``ec2:DescribeSecurityGroups``

Or attach the following AWS managed policies for quick setup:

.. code-block:: text

   AmazonECS_FullAccess
   IAMFullAccess

Step 2: Find Your Subnet IDs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ECS worker manager needs at least one subnet ID to launch Fargate tasks. Find your default VPC subnets:

.. code-block:: bash

   aws ec2 describe-subnets \
       --filters "Name=default-for-az,Values=true" \
       --query "Subnets[].SubnetId" \
       --output text

Copy one or more subnet IDs (e.g. ``subnet-0abc1234def56789a``).

Step 3: Start the Scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The scheduler must be reachable from the Fargate tasks. Use your machine's public or private IP (not ``127.0.0.1``):

.. code-block:: bash

   scaler_scheduler tcp://0.0.0.0:8516 \
       --policy-content "allocate=even_load; scaling=vanilla"


.. important::
   Fargate tasks must be able to reach the scheduler address over the network. Ensure your security group allows inbound TCP on port 8516 from the Fargate subnet CIDR, and that the scheduler binds to an accessible IP.

Step 4: Start the AWS Raw ECS Worker Manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   scaler_worker_manager aws_raw_ecs tcp://<SCHEDULER_PUBLIC_IP>:8516 \
       --ecs-subnets subnet-0abc1234def56789a \
       --aws-region us-east-1 \
       --max-task-concurrency 4 \
       --ecs-task-cpu 4 \
       --ecs-task-memory 30

Or use a TOML configuration file:

.. code-block:: bash

   $ scaler config.toml

.. code-block:: toml
   :caption: config.toml

   [[worker_manager]]
   type = "aws_raw_ecs"
   scheduler_address = "tcp://<SCHEDULER_PUBLIC_IP>:8516"
   object_storage_address = "tcp://<SCHEDULER_PUBLIC_IP>:8517"
   worker_manager_id = "wm-ecs"
   ecs_subnets = "subnet-0abc1234def56789a"
   aws_region = "us-east-1"
   max_task_concurrency = 4
   ecs_task_cpu = 4
   ecs_task_memory = 30
   ecs_cluster = "scaler-cluster"
   ecs_task_definition = "scaler-task-definition"
   ecs_task_image = "public.ecr.aws/v4u8j8r6/scaler:latest"

Step 5: Submit Tasks
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from scaler import Client

   def compute(x):
       return x ** 2

   with Client(address="tcp://<SCHEDULER_PUBLIC_IP>:8516") as client:
       futures = client.map(compute, range(50))
       results = [f.result() for f in futures]
       print(results)

How It Works
------------

1. The AWS Raw ECS worker manager connects to the Scaler scheduler and sends periodic heartbeats.
2. When the scheduler's scaling policy requests more workers, it sends a ``StartWorkerGroup`` command.
3. The worker manager calls ``ecs:RunTask`` to launch a Fargate task running the Scaler worker container.
4. Each Fargate task runs ``scaler_cluster`` inside the container, spawning one or more worker processes (controlled by ``--ecs-task-cpu``).
5. Workers connect back to the scheduler and process tasks like local workers.
6. When the scheduler wants to scale down, it sends a ``ShutdownWorkerGroup`` command and the worker manager stops the Fargate task.

Configuration Reference
------------------------

AWS Raw ECS Parameters
~~~~~~~~~~~~~~~~~~~~~~~

* ``scheduler_address`` (positional, required): Address of the Scaler scheduler. Must be reachable from Fargate tasks.
* ``--ecs-subnets`` (required): Comma-separated list of VPC subnet IDs for Fargate tasks.
* ``--aws-region``: AWS region (default: ``us-east-1``).
* ``--aws-access-key-id``: AWS access key (default: uses environment/profile).
* ``--aws-secret-access-key``: AWS secret key (default: uses environment/profile).
* ``--ecs-cluster``: ECS cluster name (default: ``scaler-cluster``). Created automatically if missing.
* ``--ecs-task-definition``: Task definition family name (default: ``scaler-task-definition``). Created automatically if missing.
* ``--ecs-task-image``: Container image (default: ``public.ecr.aws/v4u8j8r6/scaler:latest``).
* ``--ecs-task-cpu``: Number of vCPUs per Fargate task (default: ``4``). Also determines the number of worker processes per task.
* ``--ecs-task-memory``: Memory per Fargate task in GB (default: ``30``).
* ``--ecs-python-requirements``: Python packages to install in the container at startup (default: ``tomli;pargraph;parfun;pandas``).
* ``--ecs-python-version``: Python version for the container (default: ``3.12.11``).
* ``--max-task-concurrency`` (``-mtc``): Maximum number of Fargate tasks (default: number of CPUs − 1).

Common Parameters
~~~~~~~~~~~~~~~~~

For worker behavior, logging, and event loop options, see :doc:`common_parameters`.

Architecture
------------

.. code-block:: text

   ┌─────────┐     ┌───────────┐     ┌──────────────────┐     ┌─────────────────────┐
   │  Client  │────>│ Scheduler │<───>│ ECS WorkerAdapter│────>│ AWS ECS (Fargate)   │
   └─────────┘     └─────┬─────┘     └──────────────────┘     └──────────┬──────────┘
                         │                                                │
                         │            ┌──────────────────┐                │
                         └───────────>│  Object Storage  │<───────────────┘
                                      └──────────────────┘       (scaler_cluster
                                                                  runs inside each
                                                                  Fargate task)

1. The scheduler sends scaling commands (``StartWorkerGroup`` / ``ShutdownWorkerGroup``) to the ECS worker manager.
2. The worker manager calls ``ecs:RunTask`` to launch Fargate tasks running ``scaler_cluster``.
3. Workers inside each Fargate task connect back to the scheduler and process tasks like local workers.
4. The worker manager auto-creates the ECS cluster and task definition on first run if they don't exist.

Troubleshooting
---------------

**Tasks stuck in PROVISIONING:**
Check that your subnets have a route to the internet (either a public subnet with an Internet Gateway, or a private subnet with a NAT Gateway). Fargate needs internet access to pull container images.

**Workers can't connect to scheduler:**
Ensure the scheduler address is a public/private IP reachable from the Fargate subnet. Update security group inbound rules to allow TCP traffic on port 8516.

**Permission errors on RunTask:**
Ensure the ``ecsTaskExecutionRole`` IAM role exists and has the ``AmazonECSTaskExecutionRolePolicy`` attached. The worker manager creates this automatically on first run.
