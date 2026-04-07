ORB AWS EC2 Quickstart (EC2)
============================

.. _orb_aws_ec2_ec2_quick_setup:

This quickstart runs the Scaler scheduler, object storage server, and ORB worker
manager on a fresh EC2 instance. Because all services are in AWS, workers connect
to the scheduler and object storage via private VPC addresses — no NAT or
port-forwarding required. Your local machine connects to the EC2 instance using
its public IP.

Step 1 — Install Prerequisites (Local Machine)
-----------------------------------------------

Install the Scaler client package on your local machine:

.. code-block:: bash

   pip install opengris-scaler

Install AWS CLI v2:

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

Then authenticate with AWS CLI:

.. code-block:: bash

   aws login

.. note::

   If you are not using the root account, your IAM user must have the
   ``SignInLocalDevelopmentAccess`` managed policy attached to use ``aws login``.
   See :ref:`orb_aws_ec2_permissions` for full AWS permission requirements.

Click the page link and proceed in your default browser to sign in, then
follow the AWS CLI instructions in the terminal.

Step 2 — Launch an EC2 Instance
---------------------------------

The commands below use the AWS CLI to launch an AL2023 instance and configure the
security group so that:

*   You can SSH in from your local machine.
*   Your local machine can reach the scheduler (port 6788) and object storage
    server (port 6789).
*   ORB-provisioned worker instances in the same VPC can connect back to the
    scheduler and object storage using private addresses.

Run on your **local machine**:

.. code-block:: bash

   # Disable the AWS CLI pager so all commands run without interruption
   export AWS_PAGER=""

   # Discover the latest Amazon Linux 2023 AMI in your region
   AMI_ID=$(aws ec2 describe-images \
     --owners amazon \
     --filters "Name=name,Values=al2023-ami-2023.*-kernel-*-x86_64" \
               "Name=state,Values=available" \
     --query "sort_by(Images, &CreationDate)[-1].ImageId" \
     --output text)

   # Create a key pair for SSH access
   aws ec2 create-key-pair \
     --key-name scaler-key \
     --query "KeyMaterial" \
     --output text > scaler-key.pem
   chmod 400 scaler-key.pem

   # Detect your current public IP address
   MY_IP=$(curl -s https://checkip.amazonaws.com)

   # Create a security group
   SG_ID=$(aws ec2 create-security-group \
     --group-name scaler-sg \
     --description "Scaler scheduler security group" \
     --query GroupId --output text)

   # Allow SSH, scheduler (6788), and object storage (6789) from your IP
   aws ec2 authorize-security-group-ingress --group-id $SG_ID \
     --protocol tcp --port 22 --cidr $MY_IP/32
   aws ec2 authorize-security-group-ingress --group-id $SG_ID \
     --protocol tcp --port 6788 --cidr $MY_IP/32
   aws ec2 authorize-security-group-ingress --group-id $SG_ID \
     --protocol tcp --port 6789 --cidr $MY_IP/32

   # Allow all inbound traffic from EC2 private addresses (172.16.0.0/12)
   # so ORB-provisioned workers can connect back to this instance
   aws ec2 authorize-security-group-ingress --group-id $SG_ID \
     --protocol all --cidr 172.16.0.0/12

   # Launch a c5.xlarge instance (4 vCPUs, 8 GB RAM)
   INSTANCE_ID=$(aws ec2 run-instances \
     --image-id $AMI_ID \
     --instance-type c5.xlarge \
     --key-name scaler-key \
     --security-group-ids $SG_ID \
     --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=scaler-scheduler}]" \
     --query "Instances[0].InstanceId" \
     --output text)

   # Wait for the instance to be running
   aws ec2 wait instance-running --instance-ids $INSTANCE_ID

   # Retrieve the public and private IP addresses
   PUBLIC_IP=$(aws ec2 describe-instances --instance-ids $INSTANCE_ID \
     --query "Reservations[0].Instances[0].PublicIpAddress" --output text)
   PRIVATE_IP=$(aws ec2 describe-instances --instance-ids $INSTANCE_ID \
     --query "Reservations[0].Instances[0].PrivateIpAddress" --output text)

   echo "Public IP:  $PUBLIC_IP"
   echo "Private IP: $PRIVATE_IP"

Keep note of both IP addresses — you will use them in the configuration below.

Step 3 — SSH In and Install Scaler
------------------------------------

Connect to the instance:

.. code-block:: bash

   ssh -i scaler-key.pem ec2-user@$PUBLIC_IP

Then, on the EC2 instance, install Python 3.13 and Scaler:

.. code-block:: bash

   # Install Python 3.14
   sudo dnf install -y python3.13

   # Create a virtualenv and install Scaler with all extras
   python3.13 -m venv .venv
   source .venv/bin/activate
   pip install opengris-scaler[all]

Then authenticate with AWS from the remote instance:

.. code-block:: bash

   aws login --remote

Open the URL printed by the command in your local browser, complete sign-in,
then copy the returned code/token and paste it back into the remote terminal
to finish login.

Step 4 — Configure and Start Services
---------------------------------------

Create a ``stack.toml`` on the EC2 instance. Replace ``<EC2_PUBLIC_IP>`` and
``<EC2_PRIVATE_IP>`` with the values printed in Step 2.

The scheduler's ``advertised_object_storage_address`` is forwarded to connecting
clients, so it must be set to the EC2 **public** IP. The ``object_storage_address``
points to the local object storage server. The worker manager's
``object_storage_address`` and ``worker_scheduler_address`` use the EC2
**private** IP so that ORB-provisioned workers stay on the faster internal VPC
network.

.. tabs::

   .. group-tab:: stack.toml

      .. code-block:: toml

         [object_storage_server]
         bind_address = "tcp://0.0.0.0:6789"

         [scheduler]
         # bind on all interfaces so both the local client and workers can reach this instance
         bind_address = "tcp://0.0.0.0:6788"
         # connect to the local object storage server
         object_storage_address = "tcp://127.0.0.1:6789"
         # advertise the public IP as the object storage address so clients can connect from outside AWS
         advertised_object_storage_address = "tcp://<EC2_PUBLIC_IP>:6789"

         [[worker_manager]]
         type = "orb_aws_ec2"
         scheduler_address = "tcp://127.0.0.1:6788"
         worker_manager_id = "wm-orb"
         # workers run in the same VPC — use the private IP for lower latency
         worker_scheduler_address = "tcp://<EC2_PRIVATE_IP>:6788"
         object_storage_address = "tcp://<EC2_PRIVATE_IP>:6789"
         python_version = "3.14"
         requirements_txt = """
         opengris-scaler>=1.27.0
         numpy
         """
         instance_type = "t3.medium"
         aws_region = "us-east-1"
         logging_level = "INFO"

      Run command:

      .. code-block:: bash

         scaler stack.toml

   .. group-tab:: command line

      .. code-block:: bash

         scaler_object_storage_server tcp://0.0.0.0:6789 &
         scaler_scheduler tcp://0.0.0.0:6788 \
             --object-storage-address tcp://127.0.0.1:6789 \
             --advertised-object-storage-address tcp://<EC2_PUBLIC_IP>:6789 &
         scaler_worker_manager orb_aws_ec2 tcp://127.0.0.1:6788 \
             --worker-manager-id wm-orb \
             --public-scheduler-address tcp://<EC2_PRIVATE_IP>:6788 \
             --object-storage-address tcp://<EC2_PRIVATE_IP>:6789 \
             --python-version 3.14 \
             --requirements-txt $'opengris-scaler>=1.27.0\nnumpy' \
             --instance-type t3.medium \
             --aws-region us-east-1 \
             --logging-level INFO

Step 5 — Connect a Client
--------------------------

From your **local machine**, connect to the scheduler using the EC2 public IP.
The client automatically receives the object storage address from the scheduler —
no additional configuration is needed.

.. note::

   The local client must use the same Python version (3.13) and the same version
   of ``opengris-scaler`` as the EC2 instance. Version mismatches can cause
   serialization errors at runtime.

The example below uses ``numpy``, which is included in ``requirements_txt`` and
will be installed on each worker instance automatically.

.. code-block:: python

   import numpy as np
   from scaler import Client


   def sum_array(arr):
       return float(np.sum(arr))


   with Client(address="tcp://<EC2_PUBLIC_IP>:6788") as client:
       arrays = [np.random.rand(1000) for _ in range(100)]
       results = client.map(sum_array, arrays)

   print(results)
