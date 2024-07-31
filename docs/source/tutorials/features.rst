Additional Features
===================

These features are not needed for standard usage, but are helpful for monitoring and other use cases.

Scaler Top (Monitoring)
-----------------------

The scheduler has an address that can be monitored with an ipc connection. The exact address will be in the logs
when the scheduler is spun up. Connect to it with the `scaler_top` CLI command.

.. code:: bash

    scaler_top ipc:///tmp/0.0.0.0_8516_monitor

Which will show something similar to top command, but it's for getting status of the scaled system:

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


* scheduler section is showing how much resources the scheduler used
* task_manager section shows count for each task status
* scheduler_sent section shows count for each type of messages scheduler sent
* scheduler_received section shows count for each type of messages scheduler received
* object_id_to_tasks section shows task count for each object used
* worker section shows worker details, you can use shortcuts to sort by columns, the char * on column header show which
  column is sorted right now
  * agt_cpu/agt_rss means cpu/memory usage of worker agent
  * cpu/rss means cpu/memory usage of worker
  * free means number of free task slots for this worker
  * sent means how many tasks scheduler sent to the worker
  * queued means how many tasks worker received and queued
  * lag means the latency between scheduler and worker
  * ITL means debug bits information, I means processor initialized, T means have a task or not, L means task lock


Task Profiling
--------------

To get the execution time of a task, submit it with profiling turned on. ``.result()`` needs to be called on the Future first so that execution is complete.

.. code:: python

    from scaler import Client

    def calculate(sec: int):
        return sec * 1

    client = Client(address="tcp://127.0.0.1:2345")
    fut = client.submit(calculate, 1, profiling=True)
    fut.result()

    # contains task run duration time in microseconds
    fut.profiling_info().duration_us

    # contains the peak memory usage in bytes for that function, this memory peak is sampled every second
    fut.profiling_info().peak_memory


Send Object
-----------

Scaler can send objects to the workers. This is useful for sending large objects that are needed for the tasks, and
reuse it over and over again

- The object is sent to the workers only once
- the scaler API will returns an reference that link to the object
- on the workers side, workers can use this reference to access the object, but this reference must be in the
  positional argument level, **it cannot be nested to the other reference or inside of list etc.**
- This ``client.send_object``, objects are still get serialized and deserialized by Serializer, if you have special
  needs to customize your serializer, please refer below :ref:`Custom Serializer` section

.. code:: python

    from scaler import Client

    def add(a, b):
        return a + b

    client = Client(address="tcp://127.0.0.1:2345")
    ref = client.send_object("large_object", [1, 2, 3, 4, 5])

    fut = client.submit(add, ref, [6])
    assert fut.result() == [1, 2, 3, 4, 5, 6]

    # this will not work, scaler doesn't do deep resolving
    # fut = client.submit(add, [ref], [6])


Graph Submission
----------------

For tasks that are dependent on the output of other tasks, they can all be submitted together as a graph. Scaler will handle executing the dependencies in the right order.

.. code:: python

    from scaler import Client

    def inc(i):
        return i + 1

    def add(a, b):
        return a + b

    def minus(a, b):
        return a - b

    graph = {
        "a": 2,
        "b": 2,
        "c": (inc, "a"),  # c = a + 1 = 2 + 1 = 3
        "d": (add, "a", "b"),  # d = a + b = 2 + 2 = 4
        "e": (minus, "d", "c")  # e = d - c = 4 - 3 = 1
    }

    client = Client(address="tcp://127.0.0.1:2345")
    futures = client.submit_graph(graph, keys=["e"])

    print(futures[0].result())


Client Shutdown
---------------

By default, the Scheduler is running in protected mode.  For more information, check the :ref:`protected <protected>` section. If the Scheduler is not in protected mode, the Client can shutdown the Cluster by calling ``client.shutdown()``. This needs to be specifically enabled when spinning up the Scheduler.

.. code:: python

    from scaler import Client

    client = Client(address="tcp://127.0.0.1:2345")
    client.shutdown()

Custom Serializer
-----------------
Scaler uses cloudpickle by default for serialization. You can use a custom serializer by passing it to the Client.

The serializer API has only two methods: ``serialize`` and ``deserialize``, and these are responsible for

- function
- function arguments
- function results


**All libraries used for serialization must be installed on workers.**


.. py:function:: serialize(obj: Any) -> bytes

   :param obj: the object to be serialized, can be function object, argument object, or function result object
   :return: serialized bytes of the object

Serialize the object to bytes, this serialization method is called for function object and EACH argument
object and function result object, for example:


.. code:: python

    def add(a, b):
        return a + b

    client.submit(add, 1, 2)


``serialize`` will initially be called three times: once for ``add``, once for ``1``, and once for ``2``.
The result of the ``a+b`` calculation will then be serialized and sent back to the client.
The client will ``deserialize`` the result.


.. py:function:: deserialize(payload: bytes) -> Any

   :param payload: the serialized bytes of the object, can be function object, argument object, or function result object
   :return: any deserialized object

Deserialize the bytes to the original object, this de-serialize method is used to deserialize the function
object bytes and EACH serialized argument and serialized function result.


Below is an example implementation of customized serializer that deal with different types, but as you introduce
different types, you may need an enum in front of serialized bytes to indicate how to deserialize on other ends, for
example

- following example will serialize and deserialize the pd.DataFrame and Integer specially (not use cloudpickle)
- all other objects will be serialized by cloudpickle still


.. code:: python

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

