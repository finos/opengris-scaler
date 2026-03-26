Quickstart
==========

Quick Installation
------------------

.. code:: bash

    $ pip install opengris-scaler[all]

See :ref:`introduction_installation` for other install choices and optional dependencies.

Start Native Cluster
--------------------

Start a local scheduler and one native worker manager.

.. tabs::

    .. group-tab:: config.toml

        .. code-block:: toml

            [scheduler]
            scheduler_address = "tcp://127.0.0.1:8516"

            [[worker_manager]]
            type = "baremetal_native"
            scheduler_address = "tcp://127.0.0.1:8516"
            worker_manager_id = "wm-native"

        Run command:

        .. code-block:: bash

            $ scaler config.toml

    .. group-tab:: command line

        In terminal 1:

        .. code-block:: bash

            $ scaler_scheduler tcp://127.0.0.1:8516

        In terminal 2:

        .. code-block:: bash

            $ scaler_worker_manager baremetal_native tcp://127.0.0.1:8516 --worker-manager-id wm-native


For more sophisticated cluster startup options, see :ref:`scaler <cmd-scaler>`.

Start Compute Tasks
-------------------

Connect the Python client and submit tasks:

.. code:: python

    from scaler import Client


    def square(value):
        return value * value


    with Client(address="tcp://127.0.0.1:8516") as client:
        results = client.map(square, range(0, 100))

    print(results)
