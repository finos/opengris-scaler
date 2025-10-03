Configuration
=============

Scaler comes with a number of settings that can be tuned for performance. Reasonable defaults are chosen for these that will yield decent performance, but users can tune these settings to get the best performance for their use case. A full list of the available settings can be found by calling the CLI with the ``-h`` flag.

Object storage server settings
------------------------------

For the list of available settings, use the CLI command:

.. code:: bash

    scaler_object_storage_server -h


Scheduler Settings
------------------

For the list of available settings, use the CLI command:

.. code:: bash

    scaler_scheduler -h

**Protected Mode**

.. _protected:

The Scheduler is started in protected mode by default, which means that it can not be shut down by the Client. This is because multiple Clients can connect to a long-running Scheduler, and calling :py:func:`~Client.shutdown()` may inadverdently kill work that another user is executing. This is also because Scaler encourages strong decoupling between Client, Scheduler, and Workers. To turn off protected mode, start the scheduler with:

.. code:: bash

    scaler_scheduler tcp://127.0.0.1:8516 -p False

Or if using the programmatic API, pass ``protected=False``:

.. code:: python

    from scaler import SchedulerClusterCombo

    cluster = SchedulerClusterCombo(
        address=f"tcp://127.0.0.1:{port}",
        n_workers=2,
        protected=False, # this will turn off protected mode
    )

**Event Loop**

Scaler uses Python's built-in ``asyncio`` event loop by default, however users can choose to use ``uvloop`` for the event loop. ``uvloop`` is a faster implementation of the event loop and may improve performance of Scaler.

``uvloop`` needs to be installed separately:

.. code:: bash

    pip install uvloop

``uvloop`` can be set when starting the scheduler through the CLI:

.. code:: bash

    scaler_scheduler tcp://127.0.0.1:8516 -e uvloop

.. code:: python

    from scaler import SchedulerClusterCombo

    cluster = SchedulerClusterCombo(
        address=f"tcp://127.0.0.1:{port}",
        n_workers=2,
        event_loop="uvloop"
    )


Worker Settings
---------------

For the list of available settings, use the CLI command:

.. code:: bash

    scaler_cluster -h

**Preload Hook**

Workers can execute an optional initialization function before processing tasks using the ``--preload`` option. This enables workers to:

* Set up environments on demand
* Preload data, libraries, or models  
* Initialize connections or state

The preload specification follows the format ``module.path:function(args, kwargs)`` where:

* ``module.path`` is the Python module to import
* ``function`` is the callable to execute
* ``args`` and ``kwargs`` are literal values (strings, numbers, booleans, lists, dicts)

.. code:: bash

    # Simple function call with no arguments
    scaler_cluster tcp://127.0.0.1:8516 --preload "mypackage.init:setup"
    
    # Function call with arguments
    scaler_cluster tcp://127.0.0.1:8516 --preload "mypackage.init:configure('production', debug=False)"

The preload function is executed once per processor during initialization, before any tasks are processed. If the preload function fails, the error is logged and the processor will terminate.

Example preload module (``mypackage/init.py``):

.. code:: python

    import logging
    
    def setup():
        """Basic setup with no arguments"""
        logging.info("Worker initialized")
    
    def configure(environment, debug=True):
        """Setup with configuration parameters"""
        logging.info(f"Configuring for {environment}, debug={debug}")
        # Initialize connections, load models, etc.

**Death Timeout**

Workers are spun up with a ``death_timeout_seconds``, which indicates how long the worker will stay alive without being connected to a Scheduler. The default setting is 300 seconds. This is intended for the workers to clean up if the Scheduler crashes.

This can be set using the CLI:

.. code:: bash

    scaler_cluster -n 10 tcp://127.0.0.1:8516 -ds 300

Or through the programmatic API:


