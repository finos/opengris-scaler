AWS HPC Worker Adapter
======================

The AWS HPC worker adapter offloads task execution to AWS Batch, running each task as a containerized job on managed EC2 compute. This adapter is particularly useful for bursting workloads to the cloud or running tasks that require specific hardware (e.g., GPUs, high memory) not available locally. It currently supports the AWS Batch backend.

.. seealso::
    For a comprehensive, step-by-step walkthrough of setting up AWS infrastructure, building Docker images, and troubleshooting, see the :doc:`AWS Batch Setup Guide <setup>`.

Prerequisites
-------------

To use the AWS HPC worker adapter, you need:

*   An **AWS Account** with appropriate permissions.
*   The ``boto3`` Python library installed in your Scaler environment.
*   **Docker** installed (if you need to build and push the worker container image).
*   Provisioned AWS resources:

    *   An **S3 Bucket** for storing task data and results.
    *   An **AWS Batch Compute Environment** and **Job Queue**.
    *   An **AWS Batch Job Definition** pointing to a container image that includes Scaler and its dependencies.
    *   An **ECR Repository** (recommended) to host your worker container image.

Getting Started
---------------

To start the AWS HPC worker adapter from the command line:

.. code-block:: bash

    python -m scaler.entry_points.worker_adapter_aws_hpc \
        --scheduler-address tcp://<SCHEDULER_IP>:8516 \
        --job-queue my-scaler-queue \
        --job-definition my-scaler-job-def \
        --s3-bucket my-scaler-tasks-bucket \
        --aws-region us-east-1

Equivalent configuration using a TOML file:

.. code-block:: bash

    python -m scaler.entry_points.worker_adapter_aws_hpc --config config.toml

.. code-block:: toml

    # config.toml

    [aws_hpc_worker_adapter]
    job_queue = "my-scaler-queue"
    job_definition = "my-scaler-job-def"
    s3_bucket = "my-scaler-tasks-bucket"
    aws_region = "us-east-1"
    max_concurrent_jobs = 100

Key Arguments:
~~~~~~~~~~~~~~

*   ``--scheduler-address``: The address of the Scaler scheduler.
*   ``--job-queue`` (``-q``): The name of the AWS Batch job queue to submit tasks to.
*   ``--job-definition`` (``-d``): The name of the AWS Batch job definition to use.
*   ``--s3-bucket``: The S3 bucket used for task payload and result storage.
*   ``--aws-region``: The AWS region where your resources are located.

Provisioning AWS Resources
--------------------------

For development and testing, Scaler includes a provisioning utility that can build your worker image and create the necessary AWS Batch resources.

.. code-block:: bash

    python src/scaler/worker_manager_adapter/aws_hpc/utility/provisioner.py provision \
        --region us-east-1 \
        --prefix my-scaler-test

For detailed step-by-step instructions on provisioning, container setup, and testing, please refer to the :doc:`AWS Batch Setup Guide <setup>`.

How it Works
------------

*   **Payload Handling**: Task payloads are serialized using ``cloudpickle``. If the compressed payload is larger than 28KB, it is uploaded to S3. Smaller payloads are passed directly to the AWS Batch job as parameters.
*   **Execution**: The Batch container runs a specialized runner script (``batch_job_runner.py``) that deserializes the task, executes it, and writes the result (and any errors) back to S3.
*   **Concurrency**: The adapter manages a semaphore to limit the number of concurrent Batch jobs (``--max-concurrent-jobs``), preventing accidental resource exhaustion or exceeding AWS service quotas.
*   **Efficiency**: Payloads > 4KB are automatically compressed with gzip to minimize S3 usage and data transfer.

Supported Parameters
--------------------

.. note::
    For more details on how to configure Scaler, see the :doc:`../../configuration` section.

The AWS HPC worker adapter supports the following specific configuration parameters.

AWS HPC Configuration
~~~~~~~~~~~~~~~~~~~~~

*   ``--job-queue`` (``-q``): AWS Batch job queue name (required).
*   ``--job-definition`` (``-d``): AWS Batch job definition name (required).
*   ``--s3-bucket``: S3 bucket for task data (required).
*   ``--s3-prefix``: S3 prefix within the bucket (default: ``scaler-tasks``).
*   ``--aws-region``: AWS region (default: ``us-east-1``).
*   ``--max-concurrent-jobs`` (``-mcj``): Maximum number of concurrent Batch jobs (default: ``100``).
*   ``--job-timeout-minutes``: Maximum time a Batch job is allowed to run before being terminated (default: ``60``).
*   ``--backend`` (``-b``): AWS HPC backend to use (default: ``batch``).
*   ``--name`` (``-n``): A custom name for the worker adapter instance.

Common Parameters
~~~~~~~~~~~~~~~~~

For a full list of common parameters including networking, worker behavior, and logging, see :doc:`../common_parameters`.

Setup Guide
-----------

.. important::
    Setting up AWS infrastructure and ensuring correct container configuration is critical for the AWS HPC adapter to function correctly. 

For a comprehensive walkthrough of setting up AWS infrastructure, building Docker images, and troubleshooting, please refer to our detailed :doc:`AWS Batch Setup Guide <setup>`.

.. toctree::
    :maxdepth: 1
    :hidden:

    setup

