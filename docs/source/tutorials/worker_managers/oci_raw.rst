OCI Raw Worker Manager
======================

The OCI Raw worker manager provisions Scaler workers as `OCI Container Instances <https://www.oracle.com/cloud/compute/container-instances/>`_. Unlike the :doc:`OCI HPC worker manager <oci_hpc>`, which runs each Scaler *task* as a separate container, the OCI Raw worker manager launches full Scaler *worker processes* inside container instances. Workers connect back to the scheduler and process tasks the same way local workers do, with the scheduler handling load balancing and scaling.

Prerequisites
-------------

* An OCI account with a tenancy
* `OCI CLI <https://docs.oracle.com/en-us/iaas/Content/API/SDKDocs/cliinstall.htm>`_ installed and configured (``oci setup config``)
* Python packages: ``pip install opengris-scaler[oci]``
* A VCN with at least one subnet reachable from the machine running the scheduler
* An OCIR repository and a container image pushed to it (see `Build the Worker Image`_ below)

Quick Start
-----------

Install OCI CLI and configure credentials:

.. code-block:: bash

   bash -c "$(curl -L https://raw.githubusercontent.com/oracle/oci-cli/master/scripts/install/install.sh)"
   oci setup config

Create a virtual environment and install Scaler with OCI extras:

.. code-block:: bash

   python -m venv .venv
   source .venv/bin/activate
   pip install opengris-scaler[oci]

Gather your OCI resource identifiers:

.. code-block:: bash

   # Compartment OCID
   oci iam compartment list --query "data[0].id"

   # Availability domain name
   oci iam availability-domain list --query "data[0].name"

   # Subnet OCID (replace VCN_ID with your VCN OCID)
   oci network subnet list --compartment-id <COMPARTMENT_ID> --query "data[0].id"

Copy ``config.toml`` below, replace the placeholder values, then start services:

.. tabs::

   .. group-tab:: config.toml

      .. code-block:: toml
         :caption: config.toml

         [object_storage_server]
         bind_address = "tcp://127.0.0.1:8517"

         [scheduler]
         bind_address = "tcp://0.0.0.0:8516"
         object_storage_address = "tcp://127.0.0.1:8517"

         [[worker_manager]]
         type = "oci_raw"
         scheduler_address = "tcp://127.0.0.1:8516"
         worker_scheduler_address = "tcp://<PUBLIC_IP>:8516"
         object_storage_address = "tcp://<PUBLIC_IP>:8517"
         worker_manager_id = "wm-oci-raw"

         [worker_manager.container_instance_config]
         oci_region = "us-ashburn-1"
         compartment_id = "ocid1.compartment.oc1..example"
         availability_domain = "AD-1"
         subnet_id = "ocid1.subnet.oc1..example"
         container_image = "us-ashburn-1.ocir.io/<namespace>/<repo>:latest"

         [worker_manager.python_worker_environment]
         python_version = "3.12"
         requirements_txt = "opengris-scaler[oci]\ntomli\npargraph"

         instance_ocpus = 4.0
         instance_memory_gb = 30.0

      Run command:

      .. code-block:: bash

         scaler config.toml

   .. group-tab:: command line

      .. code-block:: bash

         scaler_object_storage_server tcp://127.0.0.1:8517
         scaler_scheduler tcp://0.0.0.0:8516 \
             --object-storage-address tcp://127.0.0.1:8517 \
             --policy-content "allocate=even_load; scaling=vanilla"
         scaler_worker_manager oci_raw tcp://127.0.0.1:8516 \
             --worker-scheduler-address tcp://<PUBLIC_IP>:8516 \
             --object-storage-address tcp://<PUBLIC_IP>:8517 \
             --worker-manager-id wm-oci-raw \
             --oci-region us-ashburn-1 \
             --compartment-id ocid1.compartment.oc1..example \
             --availability-domain AD-1 \
             --subnet-id ocid1.subnet.oc1..example \
             --container-image us-ashburn-1.ocir.io/<namespace>/<repo>:latest \
             --python-version 3.12 \
             --requirements-txt "opengris-scaler[oci]" \
             --instance-ocpus 4.0 \
             --instance-memory-gb 30.0

After services are up, use a client to submit tasks to OCI-provisioned workers.

.. code-block:: python
   :caption: my_client.py (Terminal 3)

   from scaler import Client

   def compute(x):
       return x ** 2

   with Client(address="tcp://<PUBLIC_IP>:8516") as client:
       futures = client.map(compute, range(50))
       print([f.result() for f in futures])

.. _oci_raw_build_worker_image:

Build the Worker Image
----------------------

The OCI Raw worker manager requires a container image that can run ``scaler_cluster``. Since the Scaler package is installed inside the container at startup from ``requirements_txt``, the base image only needs Python and standard OS packages.

.. code-block:: dockerfile
   :caption: Dockerfile

   FROM python:3.12-slim
   RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
   CMD ["bash"]

Build and push to your OCIR repository:

.. code-block:: bash

   # Authenticate with OCIR
   docker login us-ashburn-1.ocir.io -u <tenancy-namespace>/<username>

   # Build and push
   docker build -t us-ashburn-1.ocir.io/<namespace>/<repo>:latest .
   docker push us-ashburn-1.ocir.io/<namespace>/<repo>:latest

.. note::
   The ``requirements_txt`` field in the worker manager config controls what Python packages are installed in the container when it starts. Include ``opengris-scaler[oci]`` and any packages your tasks depend on.

Detailed Setup
--------------

Step 1: Configure OCI Authentication
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

   .. group-tab:: Local Machine (config_file)

      Configure the OCI CLI to read credentials from ``~/.oci/config`` (the default):

      .. code-block:: bash

         oci setup config

      Follow the prompts to enter your tenancy OCID, user OCID, region, and key path. Verify with:

      .. code-block:: bash

         oci iam user list

   .. group-tab:: OCI VM (instance_principal)

      When the worker manager runs on an OCI VM that belongs to a Dynamic Group, it can use instance principal credentials — no config file needed. Set ``auth_type = "instance_principal"`` in the config, or pass ``--auth-type instance_principal`` on the command line.

      Ensure the VM's OCID is included in a Dynamic Group policy that grants ``manage container-instances`` and ``manage virtual-network-family`` permissions in your compartment.

Step 2: Find Your Resource Identifiers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

   # List compartments
   oci iam compartment list --all --query "data[].{name:name, id:id}" --output table

   # List availability domains
   oci iam availability-domain list --compartment-id <COMPARTMENT_ID> --output table

   # List subnets in a VCN
   oci network subnet list --compartment-id <COMPARTMENT_ID> --output table

Step 3: Start the Scheduler
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The scheduler must be reachable from inside the OCI Container Instances on your subnet. Use your machine's public or OCI-internal IP (not ``127.0.0.1``):

.. code-block:: bash

   scaler_object_storage_server tcp://127.0.0.1:8517
   scaler_scheduler tcp://0.0.0.0:8516 \
       --object-storage-address tcp://127.0.0.1:8517 \
       --policy-content "allocate=even_load; scaling=vanilla"

.. important::
   Container Instances run inside OCI's network. A scheduler running on your laptop is not reachable from container instances unless it has a public IP and your VCN allows inbound traffic on port 8516. Deploy the scheduler on an OCI VM in the same VCN, or use a public IP with an appropriate security list rule.

Step 4: Start the OCI Raw Worker Manager
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tabs::

   .. group-tab:: config.toml

      .. code-block:: toml

         [[worker_manager]]
         type = "oci_raw"
         scheduler_address = "tcp://127.0.0.1:8516"
         worker_scheduler_address = "tcp://<OCI_VM_IP>:8516"
         object_storage_address = "tcp://<OCI_VM_IP>:8517"
         worker_manager_id = "wm-oci-raw"

         [worker_manager.container_instance_config]
         oci_region = "us-ashburn-1"
         compartment_id = "ocid1.compartment.oc1..example"
         availability_domain = "Uocm:US-ASHBURN-AD-1"
         subnet_id = "ocid1.subnet.oc1..example"
         container_image = "us-ashburn-1.ocir.io/<namespace>/<repo>:latest"
         instance_shape = "CI.Standard.E4.Flex"

         [worker_manager.python_worker_environment]
         python_version = "3.12"
         requirements_txt = "opengris-scaler[oci]\ntomli\npargraph\npandas"

         instance_ocpus = 4.0
         instance_memory_gb = 30.0

      Run command:

      .. code-block:: bash

         scaler config.toml

   .. group-tab:: command line

      .. code-block:: bash

         scaler_worker_manager oci_raw tcp://127.0.0.1:8516 \
             --worker-scheduler-address tcp://<OCI_VM_IP>:8516 \
             --object-storage-address tcp://<OCI_VM_IP>:8517 \
             --worker-manager-id wm-oci-raw \
             --oci-region us-ashburn-1 \
             --compartment-id ocid1.compartment.oc1..example \
             --availability-domain "Uocm:US-ASHBURN-AD-1" \
             --subnet-id ocid1.subnet.oc1..example \
             --container-image us-ashburn-1.ocir.io/<namespace>/<repo>:latest \
             --python-version 3.12 \
             --requirements-txt "opengris-scaler[oci]" \
             --instance-ocpus 4.0 \
             --instance-memory-gb 30.0

Step 5: Submit Tasks
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   from scaler import Client

   def compute(x):
       return x ** 2

   with Client(address="tcp://<OCI_VM_IP>:8516") as client:
       futures = client.map(compute, range(50))
       results = [f.result() for f in futures]
       print(results)

How It Works
------------

1. The OCI Raw worker manager connects to the Scaler scheduler and sends periodic heartbeats.
2. On each heartbeat, the scheduler responds with a ``setDesiredTaskConcurrency`` command declaring the target worker count per capability set.
3. The worker manager converges by calling the OCI Container Instances API to launch or stop container instances.
4. Each container instance installs the packages from ``requirements_txt`` at startup, then runs ``scaler_cluster`` to spawn one or more worker processes. The number of workers per instance is determined by ``instance_ocpus``.
5. Workers connect back to the scheduler (via ``worker_scheduler_address``) and process tasks like local workers.

Configuration Reference
------------------------

OCI Raw Parameters
~~~~~~~~~~~~~~~~~~

* ``scheduler_address`` (positional, required): Address of the Scaler scheduler.
* ``--worker-manager-id`` (``-wmi``, required): Unique identifier for this worker manager instance.
* ``--worker-scheduler-address``: Scheduler address used by workers inside container instances. Must be reachable from OCI (default: same as ``scheduler_address``).
* ``--object-storage-address``: Object storage address used by workers. Must be reachable from OCI.

Container Instance Config (``[worker_manager.container_instance_config]``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``--oci-region``: OCI region identifier (default: ``us-ashburn-1``).
* ``--compartment-id`` (required): OCI Compartment OCID where container instances are launched.
* ``--availability-domain`` (required): OCI Availability Domain (e.g. ``AD-1`` or ``Uocm:US-ASHBURN-AD-1``).
* ``--subnet-id`` (required): Subnet OCID for container instance network interfaces.
* ``--container-image`` (required): OCIR image URI (e.g. ``us-ashburn-1.ocir.io/<ns>/<repo>:latest``).
* ``--instance-shape``: Container instance shape (default: ``CI.Standard.E4.Flex``).
* ``--auth-type``: OCI authentication mode — ``config_file`` (default) or ``instance_principal``.
* ``--oci-profile``: OCI config file profile name (default: ``DEFAULT``).

Python Worker Environment (``[worker_manager.python_worker_environment]``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``--python-version`` (required): Python version for workers (e.g. ``3.12``).
* ``--requirements-txt`` (required): Packages to install in the container at startup. Must include ``opengris-scaler[oci]``. Can be an inline newline-separated string or a path to a file.

Sizing Parameters
~~~~~~~~~~~~~~~~~

* ``--instance-ocpus``: Number of OCPUs per container instance (default: ``4.0``). Also determines the number of worker processes started per instance.
* ``--instance-memory-gb``: Memory in GB per container instance (default: ``30.0``).

Common Parameters
~~~~~~~~~~~~~~~~~

For worker behavior, logging, and event loop options, see :doc:`common_parameters`.

Architecture
------------

.. code-block:: text

   ┌─────────┐     ┌───────────┐     ┌──────────────────────┐     ┌──────────────────────────┐
   │  Client │────>│ Scheduler │<───>│ OCI Raw WorkerManager│────>│ OCI Container Instances  │
   └─────────┘     └─────┬─────┘     └──────────────────────┘     └────────────┬─────────────┘
                         │                                                      │
                         │            ┌──────────────────┐                     │
                         └───────────>│  Object Storage  │<────────────────────┘
                                      └──────────────────┘       (scaler_cluster
                                                                  runs inside each
                                                                  container instance)

1. The scheduler sends a ``setDesiredTaskConcurrency`` command to the worker manager on each heartbeat.
2. The worker manager calls the OCI Container Instances API to launch instances running ``scaler_cluster``.
3. Workers inside each instance connect back to the scheduler and process tasks.
4. When workers are no longer needed, the worker manager stops the corresponding container instances.

Troubleshooting
---------------

**Workers can't connect to scheduler:**
Container Instances run inside OCI's network. Ensure ``worker_scheduler_address`` is a public or OCI-internal IP reachable from your subnet, not ``127.0.0.1``. Check your VCN security list to allow inbound TCP on port 8516 from the container instance subnet.

**Container instances fail to start:**
Check OCI Console → Container Instances for error messages. Common causes: invalid subnet or compartment OCID, missing OCIR pull secret, or insufficient IAM permissions. Ensure your user/Dynamic Group has ``manage container-instances`` and ``read virtual-network-family`` policies in the compartment.

**``scaler_cluster`` not found in container:**
Ensure ``requirements_txt`` includes ``opengris-scaler[oci]``. The entrypoint installs it at container startup before launching workers.

**Image pull errors:**
Authenticate Docker to your OCIR region and ensure the image URI in ``container_image`` matches the pushed image exactly. For private repositories, confirm the container instance's subnet can reach the OCIR endpoint.
