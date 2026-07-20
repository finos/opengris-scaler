Security
========

Scaler supports encryption of network traffic using SSL/TLS through the use of ``tls://`` or ``wss://`` (secure
WebSocket) prefixed addresses.

Step 1 -- Generate a self-signed certificate (optional)
-------------------------------------------------------

.. warning::

   A self-signed certificate as shown in this tutorial provides **encryption only**. It does **not** authenticate peers
   and offers no protection against an active man-in-the-middle attacker.

   For production deployments, use certificates issued by a trusted certificate authority (CA).

Use a single OpenSSL command to create a certificate chain and private key:

.. code-block:: bash

    openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -days 365 -nodes -subj "/CN=localhost"

This produces ``cert.pem`` (the certificate chain) and ``key.pem`` (the private key).


Step 2 -- Secured cluster configuration
---------------------------------------

Create a single ``secure_config.toml`` file.

It mirrors a plain cluster definition but uses ``tls://`` (or ``wss://``) for the binding addresses and adds the
``tls_cert`` and ``tls_key`` keys to the binding components (object storage server and scheduler).

Connecting components such as the worker manager and client will reach the scheduler over ``tls://`` and need no
certificate.

.. code-block:: toml

    [object_storage_server]
    bind_address = "tls://127.0.0.1:8527"
    tls_cert = "cert.pem"
    tls_key = "key.pem"

    [scheduler]
    bind_address = "tls://127.0.0.1:8526"
    object_storage_address = "tls://127.0.0.1:8527"
    tls_cert = "cert.pem"
    tls_key = "key.pem"

    [[worker_manager]]
    type = "baremetal_native"
    scheduler_address = "tls://127.0.0.1:8526"
    worker_manager_id = "wm-native"

Step 3 -- Launch the cluster
----------------------------

Start the object storage server, scheduler, and workers with a single command:

.. code-block:: bash

    scaler secure_config.toml

.. note::

   The same ``--tls-cert`` and ``--tls-key`` flags are available on the individual ``scaler_scheduler``,
   ``scaler_object_storage_server``, ``scaler_gui`` and  ``scaler_top`` commands when you launch components separately.

Step 4 -- Connect a client over TLS
-----------------------------------

The client connects over a ``tls://`` address and needs no certificate:

.. code-block:: python

    from scaler.client.client import Client

    with Client(address="tls://127.0.0.1:8526") as client:
        print(client.submit(round, 3.14).result())
