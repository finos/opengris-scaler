Additional Features
===================

Scaler comes with a number of additional features that can be used to monitor and profile tasks, and customize behavior.

Scaler Top (Monitoring)
-----------------------

Top is a monitoring tool that allows you to see the status of the Scaler.
The scheduler prints an address to the logs on startup that can be used to connect to it with the `scaler_top` CLI command:

.. code:: bash

    scaler_top ipc:///tmp/0.0.0.0_8516_monitor

Which will show an interface similar to the standard Linux `top` command:

.. code:: console

   scheduler        | task_manager        |     scheduler_sent         | scheduler_received
         cpu   0.0% |   unassigned      0 |      HeartbeatEcho 283,701 |          Heartbeat 283,701
         rss 130.1m |      running      0 |     ObjectResponse     233 |      ObjectRequest     215
                    |      success 53,704 |           TaskEcho  53,780 |               Task  53,764
                    |       failed     14 |               Task  54,660 |         TaskResult  53,794
                    |     canceled     48 |         TaskResult  53,766 |  DisconnectRequest      21
                    |    not_found     14 |      ObjectRequest     366 |         TaskCancel      60
                                          | DisconnectResponse      21 |    BalanceResponse      15
                                          |         TaskCancel      62 |          GraphTask       6
                                          |     BalanceRequest      15 |
                                          |    GraphTaskResult       6 |
   -------------------------------------------------------------------------------------------------
   Shortcuts: worker[n] agt_cpu[C] agt_rss[M] cpu[c] rss[m] free[f] sent[w] queued[d] lag[l]

   Total 7 worker(s)
                      worker agt_cpu agt_rss [cpu]    rss free sent queued   lag ITL |    client_manager
   2732890|sd-1e7d-dfba|d26+    0.5%  111.8m  0.5% 113.3m 1000    0      0 0.7ms 100 |
   2732885|sd-1e7d-dfba|56b+    0.0%  111.0m  0.5% 111.2m 1000    0      0 0.7ms 100 | func_to_num_tasks
   2732888|sd-1e7d-dfba|108+    0.0%  111.7m  0.5% 111.0m 1000    0      0 0.6ms 100 |
   2732891|sd-1e7d-dfba|149+    0.0%  113.0m  0.0% 112.2m 1000    0      0 0.9ms 100 |
   2732889|sd-1e7d-dfba|211+    0.5%  111.7m  0.0% 111.2m 1000    0      0   1ms 100 |
   2732887|sd-1e7d-dfba|e48+    0.5%  112.6m  0.0% 111.0m 1000    0      0 0.9ms 100 |
   2732886|sd-1e7d-dfba|345+    0.0%  111.5m  0.0% 112.8m 1000    0      0 0.8ms 100 |


* `scheduler` section shows the scheduler's resource usage
* `task_manager` section shows the status of tasks
* `scheduler_sent` section counts the number of each type of message sent by the scheduler
* `scheduler_received` section counts the number of each type of message received by the scheduler
* `worker` section shows worker details, you can use shortcuts to sort by columns, and the * in the column header shows which column is being used for sorting

  * `agt_cpu/agt_rss` means cpu/memory usage of the worker agent
  * `cpu/rss` means cpu/memory usage of the worker
  * `free` means number of free task slots for the worker
  * `sent` means how many tasks scheduler sent to the worker
  * `queued` means how many tasks worker received and enqueued
  * `lag` means the latency between scheduler and the worker
  * `ITL` means is debug information

    * `I` means processor initialized
    * `T` means have a task or not
    * `L` means task lock


Task Profiling
--------------

To get the execution time of a task, submit it with profiling turned on.

We need to call ``fut.result()`` to ensure that the task has been completed.

.. code:: python

    from scaler import Client

    def calculate(sec: int):
        return sec * 1

    client = Client(address="tcp://127.0.0.1:2345")
    fut = client.submit(calculate, 1, profiling=True)

    # this will execute the task
    fut.result()

    # contains task run duration time in microseconds
    fut.profiling_info().duration_us

    # contains the peak memory usage in bytes for the task, this memory peak is sampled every second
    fut.profiling_info().peak_memory


Send Object
-----------

Scaler can send objects to the workers. This is useful for sending large objects that are needed for the tasks, and are reused frequently. This allows you to avoid the overhead of sending the object multiple times.

* The object is sent to the workers only once
* The Scaler API will return a special reference to the object
* Workers can use this reference to access the object, but this reference must be provided as a positional argument
  * `The reference cannot be nested in another reference or inside a list, etc.`

.. literalinclude:: ../../../examples/send_object_client.py
   :language: python


Graph Submission
----------------

Some tasks are complex and depend on the output of other tasks. Scaler supports submitting tasks as a  `DAG <https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_ graph and will handle executing the dependencies in the correct order and communicating between tasks.

.. literalinclude:: ../../../examples/graphtask_client.py
   :language: python

Nested Tasks
-------------

Tasks can depend on other tasks' result without using graph.

.. literalinclude:: ../../../examples/nested_client.py
   :language: python

Nested Client Scheduler Address
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When creating a ``Client`` inside a task (nested client), the scheduler ``address`` parameter is optional. If omitted, the client automatically detects and uses the scheduler address from the worker context. This is particularly useful when workers are behind firewalls or NAT, where the scheduler address accessible from inside the worker may differ from external addresses.

**Automatic Address Detection:**

.. code:: python

    from scaler import Client

    def nested_task():
        # Automatically uses worker's scheduler address if no address is provided to Client()
        with Client() as client:
            result = client.submit(lambda x: x * 2, 5).result()
        return result

    # Main client
    client = Client(address="tcp://127.0.0.1:2345")
    future = client.submit(nested_task)
    print(future.result())  # 10

Dynamically Building Graph Within a Task
----------------------------------------

When the execution graph is undetermined until runtime, one may build graph dynamically on remote end.

.. literalinclude:: ../../../examples/graphtask_nested_client.py
   :language: python

Capability Allocation
---------------------

Scaler provides an *experimental* task routing and capability management, allowing you to specify capability
requirements for tasks and allocate them to workers supporting these.

.. literalinclude:: ../../../examples/task_capabilities.py
   :language: python

Client Disconnect and Shutdown
------------------------------

By default, the Scheduler runs in protected mode. For more information, see the :ref:`protected <protected>` section.

If the Scheduler is not in protected mode, the Client can shutdown the Cluster by calling :py:func:`~Client.shutdown()`.

.. literalinclude:: ../../../examples/disconnect_client.py
   :language: python

Custom Serialization
--------------------

Scaler uses ``cloudpickle`` by default for serialization. You can use a custom serializer by passing it to the Client.

The serializer API has only two methods: ``serialize`` and ``deserialize``, and these are responsible for serializing and deserializing functions, function arguments, and function results.

.. note::
    All libraries used for serialization must be installed on workers.


.. py:function:: serialize(obj: Any) -> bytes

   :param obj: the object to be serialized, can be function object, argument object, or function result object
   :return: serialized bytes of the object

Serialize the object to bytes. This serialization method is called for the function object each argument, and function's result, for example:


.. code:: python

    def add(a, b):
        return a + b

    client.submit(add, 1, 2)


``serialize`` will be called four times:

* Once for the ``add`` function
* Once for the argument ``1``
* Once for ``2``
* Once for the result of the task

The client will then ``deserialize`` the result.


.. py:function:: deserialize(payload: bytes) -> Any

   :param payload: the serialized bytes of the object, can be function object, argument object, or function result object
   :return: any deserialized object

Deserialize the bytes into the original object, this deserialize method is used to deserialize the function and each argument as received by the workers, and the result of the task as received by the client.

Below is an example implementation of a custom serializer that uses a different serialization/deserialization method for different types of objects. It uses a simple tagging system to indicate the type of object being serialized/deserialized.

* Dataframes are serialized into the parquet format
* Integers are serialized as 4-byte integers
* All other objects are serialized using cloudpickle


.. testcode:: python

    import enum
    import pickle
    import struct
    from io import BytesIO
    from typing import Any

    import pandas as pd
    from cloudpickle import cloudpickle

    from scaler import Serializer


    class ObjType(enum.Enum):
        General = b"G"
        Integer = b"I"
        DataFrame = b"D"


    class CustomSerializer(Serializer):
        @staticmethod
        def serialize(obj: Any) -> bytes:
            if isinstance(obj, pd.DataFrame):
                buf = BytesIO()
                obj.to_parquet(buf)
                return ObjType.DataFrame.value + buf.getvalue()

            if isinstance(obj, int):
                return ObjType.Integer.value + struct.pack("I", obj)

            return ObjType.General.value + cloudpickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

        @staticmethod
        def deserialize(payload: bytes) -> Any:
            obj_type = ObjType(payload[0])
            payload = payload[1:]

            if obj_type == ObjType.DataFrame:
                buf = BytesIO(payload)
                return pd.read_parquet(buf)

            if obj_type == ObjType.Integer:
                return struct.unpack("I", payload)[0]

            return cloudpickle.loads(payload)
