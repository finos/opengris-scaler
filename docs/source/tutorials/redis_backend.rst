Redis Backend for Object Storage
==================================

The Redis backend is an optional storage backend for Scaler's object storage system.
It enables distributed object sharing across multiple nodes and optional persistence of objects.

Features
--------

* **Distributed Access** - Share objects across multiple Scaler nodes
* **Optional Persistence** - Configure Redis for disk persistence
* **Automatic Serialization** - Transparent data and metadata handling
* **Connection Pooling** - Efficient Redis connection management
* **Size Limits** - Configurable max object size protection
* **Key Namespacing** - Prefix support to avoid conflicts

Installation
------------

Install Redis Backend Support
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Install Scaler with Redis support
    pip install opengris-scaler[redis]

    # Or if you already have Scaler installed
    pip install redis>=5.0.0

Install Redis Server
~~~~~~~~~~~~~~~~~~~~

**Ubuntu/Debian:**

.. code-block:: bash

    sudo apt update
    sudo apt install redis-server
    sudo systemctl start redis-server

**Docker:**

.. code-block:: bash

    docker run -d -p 6379:6379 redis:latest

Quick Start
-----------

1. Configure Redis Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a configuration file ``config.toml``:

.. code-block:: toml

    [object_storage_server]
    object_storage_address = "tcp://127.0.0.1:2346"
    backend = "redis"

    [object_storage_server.redis]
    url = "redis://localhost:6379/0"
    max_object_size_mb = 100

2. Start Object Storage Server
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    scaler_object_storage_server --config config.toml

3. Use in Your Application
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from scaler import Client

    # Connect to scheduler (which uses Redis-backed object storage)
    with Client(address="tcp://127.0.0.1:2345") as client:
        # Objects are automatically stored in Redis
        result = client.submit(lambda x: x * 2, 21)
        print(result.result())  # 42

Configuration
-------------

Basic Configuration
~~~~~~~~~~~~~~~~~~~

.. code-block:: toml

    [object_storage_server]
    backend = "redis"

    [object_storage_server.redis]
    url = "redis://localhost:6379/0"
    max_object_size_mb = 100
    key_prefix = "scaler:obj:"
    connection_pool_size = 10

Configuration Options
~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 20 15 25 40

   * - Option
     - Type
     - Default
     - Description
   * - ``url``
     - string
     - ``redis://localhost:6379/0``
     - Redis connection URL
   * - ``max_object_size_mb``
     - int
     - ``100``
     - Maximum object size in MB
   * - ``key_prefix``
     - string
     - ``scaler:obj:``
     - Prefix for Redis keys
   * - ``connection_pool_size``
     - int
     - ``10``
     - Connection pool size

Redis URL Format
~~~~~~~~~~~~~~~~

.. code-block:: text

    redis://[username:password@]host:port/database

    Examples:
      redis://localhost:6379/0                    # Local, no auth
      redis://:password@localhost:6379/0          # With password
      redis://user:pass@redis.example.com:6379/0  # With user & password
      rediss://localhost:6380/0                   # SSL/TLS connection

Advanced Configuration
----------------------

With Authentication
~~~~~~~~~~~~~~~~~~~

.. code-block:: toml

    [object_storage_server.redis]
    url = "redis://:secure_password@localhost:6379/0"

With Custom Settings
~~~~~~~~~~~~~~~~~~~~

.. code-block:: toml

    [object_storage_server.redis]
    url = "redis://localhost:6379/5"  # Use database 5
    max_object_size_mb = 50            # Limit to 50MB
    key_prefix = "myapp:scaler:obj:"  # Custom prefix
    connection_pool_size = 20          # More connections

Multiple Nodes Sharing Redis
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Node 1 config:**

.. code-block:: toml

    [object_storage_server]
    object_storage_address = "tcp://0.0.0.0:2346"
    backend = "redis"

    [object_storage_server.redis]
    url = "redis://redis-server.example.com:6379/0"

**Node 2 config:**

.. code-block:: toml

    [object_storage_server]
    object_storage_address = "tcp://0.0.0.0:2346"
    backend = "redis"

    [object_storage_server.redis]
    url = "redis://redis-server.example.com:6379/0"

Both nodes now share the same object storage!

Usage Examples
--------------

Running the Demo
~~~~~~~~~~~~~~~~

.. code-block:: bash

    # Ensure Redis is running
    redis-cli ping  # Should return: PONG

    # Run the demo script
    python examples/redis_backend_demo.py

Programmatic Usage
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from scaler.object_storage.redis_backend import RedisObjectStorageBackend

    # Create backend
    backend = RedisObjectStorageBackend(
        redis_url="redis://localhost:6379/0",
        max_object_size_mb=100
    )

    # Store object
    object_id = b"my_object"
    data = b"Hello, World!"
    metadata = b"text/plain"
    backend.put(object_id, data, metadata)

    # Retrieve object
    result = backend.get(object_id)
    if result:
        data, metadata = result
        print(f"Data: {data.decode()}")
        print(f"Metadata: {metadata.decode()}")

    # Check existence
    if backend.exists(object_id):
        print("Object exists!")

    # Delete object
    backend.delete(object_id)

    # Get info
    info = backend.get_info()
    print(f"Object count: {info['object_count']}")
    print(f"Total size: {info['total_size_bytes']} bytes")

