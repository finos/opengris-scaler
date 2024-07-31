Configuration
=============

Scaler has some settings that can be tuned for performance, but ideally users should not have to concern themselves with these because the defaults will give decent performance. A full list of the settings can be found using `-h` flag in the CLI (as detailed below). List in this page are the expected settings that users may want to configure.

Scheduler Settings
------------------

For the full list, use the CLI command:

.. code:: bash

    scaler_scheduler -h

**Protected Mode**

.. _protected:

The Scheduler is started in protected mode by default, which means that it can not be shut down by the Client. This is because multiple Clients can connect to a long-running Scheduler, and calling `Client.shutdown()` may inadverdently kill work that another user is executing. This is also because Scaler encourages strong decoupling between Client, Scheduler and Workers. To turn off protected mode, start the scheduler with:

.. code:: bash

    scaler_scheduler tcp://127.0.0.1:8516 -p False

Or if using the programmatic API, pass ``protected=True```:

.. code:: python

    from scaler import SchedulerClusterCombo

    cluster = SchedulerClusterCombo(
        address=f"tcp://127.0.0.1:{port}",
        n_workers=2,
        protected=protected,
    )

**Event Loop**

Scaler supports ``uvloop`` as the event loop for the backend. This will provide speedups when running Scaler. ``Uvloop`` needs to be installed separately:

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

For the full list, use the CLI command:

.. code:: bash

    scaler_cluster -h

**Death Timeout**

Workers are spun up with a ``death_timeout_seconds``, which indicates how long the worker will stay alive without being connected to a Scheduler. The default setting is 300 seconds. This is intended for the workers to clean up if the Scheduler crashes.

This can be set using the CLI:

.. code:: bash

    scaler_cluster -n 10 tcp://127.0.0.1:8516 -ds 300

Through the programmatic API:

.. code:: python

    from scaler import SchedulerClusterCombo

    cluster = SchedulerClusterCombo(
        address=f"tcp://127.0.0.1:8516",
        n_workers=2,
        death_timeout_seconds=300
    )

