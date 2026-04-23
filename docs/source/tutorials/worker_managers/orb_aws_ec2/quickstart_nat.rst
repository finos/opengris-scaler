ORB AWS EC2 Quickstart (NAT)
============================

.. note::

   This quickstart runs the scheduler and worker manager on your **local machine**
   and requires NAT or port-forwarding so that AWS workers can connect back to it.
   If your machine does not have a public IP or you prefer a simpler setup, see
   :doc:`quickstart_ec2` instead.

.. _orb_aws_ec2_nat_quick_setup:

Install AWS CLI
---------------

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

Start Services
--------------

Before starting services, make sure NAT setup is complete. If this machine
already has a public IP, you can ignore NAT setup. Then copy the
``config.toml`` below and replace ``PUBLIC_IP`` with your real public IP
address.

.. tabs::

   .. group-tab:: config.toml

      .. code-block:: toml

         [object_storage_server]
         bind_address = "tcp://127.0.0.1:8517"

         [scheduler]
         # bind to 0.0.0.0 so NAT can forward traffic from your public IP to this machine
         bind_address = "tcp://0.0.0.0:8516"
         object_storage_address = "tcp://127.0.0.1:8517"

         [[worker_manager]]
         type = "orb_aws_ec2"
         scheduler_address = "tcp://127.0.0.1:8516"
         worker_manager_id = "wm-orb"
         # workers provisioned in AWS need to reach your PUBLIC_IP, from where your
         # router forwards packets to the machine running the services
         object_storage_address = "tcp://<PUBLIC_IP>:8517"
         worker_scheduler_address = "tcp://<PUBLIC_IP>:8516"
         # You can start either with pre-built AMI or with specified python version
         # and requirements_txt
         # image_id = "ami-..."
         python_version = "3.14"
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

         scaler_object_storage_server tcp://127.0.0.1:8517
         scaler_scheduler tcp://0.0.0.0:8516 --object-storage-address tcp://127.0.0.1:8517
         scaler_worker_manager orb_aws_ec2 tcp://127.0.0.1:8516 \
             --worker-manager-id wm-orb \
             --worker-scheduler-address tcp://<PUBLIC_IP>:8516 \
             --object-storage-address tcp://<PUBLIC_IP>:8517 \
             --python-version 3.14 \
             --requirements-txt /path/to/requirements.txt \
             --instance-type t3.medium \
             --aws-region us-east-1 \
             --logging-level INFO \
             --task-timeout-seconds 60

After services are up, you should be good to go and can use the client to
submit tasks to workers provisioned on AWS.
