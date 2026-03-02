AWS Batch Worker Adapter
========================

The AWS Batch worker adapter offloads task execution to `AWS Batch <https://aws.amazon.com/batch/>`_, running each Scaler task as a containerized job on managed EC2 compute. Use this adapter when you need to burst workloads to the cloud, access specific hardware (GPUs, high memory), or run long-running jobs at scale.

Quick Start
-----------

Prerequisites
~~~~~~~~~~~~~

* An AWS account
* `AWS CLI <https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html>`_ installed and configured (``aws configure``)
* `Docker <https://docs.docker.com/get-docker/>`_ installed (for building the worker container image)
* Python packages: ``pip install opengris-scaler boto3``

Step 1: Configure AWS Credentials
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   aws configure
   # Enter your AWS Access Key ID, Secret Access Key, region (e.g. us-east-1), and output format (json)

Your IAM user needs the following permissions:

* **S3**: ``s3:CreateBucket``, ``s3:PutObject``, ``s3:GetObject``, ``s3:DeleteObject``, ``s3:PutLifecycleConfiguration``
* **IAM**: ``iam:CreateRole``, ``iam:AttachRolePolicy``, ``iam:PutRolePolicy``, ``iam:CreateInstanceProfile``, ``iam:AddRoleToInstanceProfile``, ``iam:GetRole``, ``iam:PassRole``
* **Batch**: ``batch:CreateComputeEnvironment``, ``batch:CreateJobQueue``, ``batch:RegisterJobDefinition``, ``batch:SubmitJob``, ``batch:DescribeJobs``, ``batch:DescribeComputeEnvironments``, ``batch:DescribeJobQueues``, ``batch:TerminateJob``, ``batch:DeregisterJobDefinition``
* **ECR**: ``ecr:CreateRepository``, ``ecr:GetAuthorizationToken``, ``ecr:PutLifecyclePolicy``, ``ecr:BatchDeleteImage``
* **EC2**: ``ec2:DescribeSubnets``, ``ec2:DescribeSecurityGroups``
* **CloudWatch Logs**: ``logs:CreateLogGroup``, ``logs:PutRetentionPolicy``, ``logs:GetLogEvents``

Or attach the following AWS managed policies for quick setup:

.. code-block:: text

   AmazonS3FullAccess
   AWSBatchFullAccess
   AmazonEC2ContainerRegistryFullAccess
   IAMFullAccess
   CloudWatchLogsFullAccess

Step 2: Provision AWS Resources
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Scaler includes a provisioner script that creates all required AWS infrastructure (S3 bucket, IAM roles, EC2 compute environment, job queue, job definition, and ECR repository):

.. code-block:: bash

   python -m scaler.worker_manager_adapter.aws_hpc.utility.provisioner provision \
       --region us-east-1 \
       --prefix scaler-batch \
       --vcpus 1 \
       --memory 2048 \
       --max-vcpus 256

This will:

1. Build and push a Docker worker image to ECR
2. Create an S3 bucket for task payloads and results
3. Create IAM roles with the minimum required permissions
4. Create an EC2 compute environment and job queue
5. Register a Batch job definition

The provisioner saves the configuration to a JSON file and an env file. Source the env file:

.. code-block:: bash

   source tests/worker_manager_adapter/aws_hpc/.scaler_aws_hpc.env

Step 3: Start the Scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   scaler_scheduler tcp://0.0.0.0:8516

.. note::
   The scheduler address must be reachable from the machine running the AWS Batch adapter. Use ``0.0.0.0`` to bind to all interfaces, or your machine's public/private IP.

Step 4: Start the AWS Batch Adapter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   scaler_worker_manager_aws_hpc_batch tcp://<SCHEDULER_IP>:8516 \
       --job-queue "$SCALER_JOB_QUEUE" \
       --job-definition "$SCALER_JOB_DEFINITION" \
       --s3-bucket "$SCALER_S3_BUCKET" \
       --aws-region "$SCALER_AWS_REGION"

Or use a TOML configuration file:

.. code-block:: bash

   scaler_worker_manager_aws_hpc_batch tcp://<SCHEDULER_IP>:8516 --config config.toml

.. code-block:: toml
   :caption: config.toml

   [aws_hpc_worker_adapter]
   job_queue = "scaler-batch-queue"
   job_definition = "scaler-batch-job"
   s3_bucket = "scaler-batch-123456789012-us-east-1"
   aws_region = "us-east-1"
   max_concurrent_jobs = 100
   job_timeout_minutes = 60

Step 5: Submit Tasks
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from scaler import Client

   def heavy_computation(x):
       return x ** 2

   with Client(address="tcp://<SCHEDULER_IP>:8516") as client:
       futures = client.map(heavy_computation, range(50))
       results = [f.result() for f in futures]
       print(results)

Cleanup
~~~~~~~

To tear down all provisioned AWS resources:

.. code-block:: bash

   python -m scaler.worker_manager_adapter.aws_hpc.utility.provisioner cleanup \
       --region us-east-1 \
       --prefix scaler-batch

How It Works
------------

1. The adapter connects to the Scaler scheduler as a worker and receives tasks.
2. Each task is serialized with ``cloudpickle`` and either passed inline (≤ 28 KB) or uploaded to S3.
3. The adapter submits an AWS Batch job for each task.
4. Inside the Batch container, a runner script (``batch_job_runner.py``) deserializes the task, executes the function, and writes the result to S3.
5. The adapter polls for job completion, fetches the result from S3, and returns it to the scheduler.

Payloads larger than 4 KB are automatically compressed with gzip. A semaphore limits concurrent Batch jobs to prevent exceeding AWS service quotas.

Configuration Reference
------------------------

AWS Batch Parameters
~~~~~~~~~~~~~~~~~~~~

* ``scheduler_address`` (positional, required): Address of the Scaler scheduler.
* ``--job-queue`` (``-q``, required): AWS Batch job queue name.
* ``--job-definition`` (``-d``, required): AWS Batch job definition name.
* ``--s3-bucket`` (required): S3 bucket for task payloads and results.
* ``--aws-region``: AWS region (default: ``us-east-1``).
* ``--s3-prefix``: S3 key prefix (default: ``scaler-tasks``).
* ``--max-concurrent-jobs`` (``-mcj``): Max concurrent Batch jobs (default: ``100``).
* ``--job-timeout-minutes``: Max job runtime in minutes (default: ``60``).
* ``--backend`` (``-b``): HPC backend (default: ``batch``).
* ``--name`` (``-n``): Custom name for the adapter instance.

Common Parameters
~~~~~~~~~~~~~~~~~

For networking, worker behavior, logging, and event loop options, see :doc:`common_parameters`.

Provisioner Reference
---------------------

The provisioner supports these commands:

.. code-block:: text

   provision      Create all AWS resources and push Docker image
   cleanup        Tear down all AWS resources
   show           Display saved configuration
   build-image    Build and push Docker image only

Provisioner flags:

* ``--region``: AWS region (default: ``us-east-1``)
* ``--prefix``: Resource name prefix (default: ``scaler-batch``)
* ``--image``: Pre-built container image (skips Docker build if provided)
* ``--vcpus``: vCPUs per job (default: ``1``)
* ``--memory``: Memory per job in MB (default: ``2048``)
* ``--max-vcpus``: Max vCPUs for compute environment (default: ``256``)
* ``--instance-types``: Comma-separated EC2 instance types (default: ``default_x86_64``)
* ``--job-timeout``: Job timeout in minutes (default: ``60``)

Troubleshooting
---------------

**Jobs stuck in RUNNABLE:**
Check that your compute environment has sufficient capacity (``--max-vcpus``) and that subnets have internet access for pulling container images.

**Permission errors:**
Ensure the IAM role attached to the job definition has S3 read/write access to the task bucket. The provisioner creates this automatically.

**Credential expiration:**
The adapter auto-refreshes expired AWS credentials. If using temporary credentials, ensure your session token is valid.
