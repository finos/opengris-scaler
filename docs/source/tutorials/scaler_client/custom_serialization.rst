Custom Serialization
====================

Scaler uses ``cloudpickle`` by default. You can provide a custom serializer on the client to control
how functions, arguments, and results are encoded and decoded.

.. note::
   Any libraries used by your serializer must be installed on workers.

Serializer interface
--------------------

Implement two methods:

.. py:function:: serialize(obj: Any) -> bytes

   :param obj: function object, argument object, or task result object
   :return: serialized bytes

.. py:function:: deserialize(payload: bytes) -> Any

   :param payload: serialized bytes produced by ``serialize``
   :return: reconstructed object

For a call like ``client.submit(add, 1, 2)``, ``serialize`` is called for:

* The function ``add``
* Argument ``1``
* Argument ``2``
* The returned result

Example implementation
----------------------

This example uses tagged payloads and mixes strategies:

* DataFrames -> parquet bytes
* Integers -> packed binary
* All other objects -> cloudpickle

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
                buffer = BytesIO()
                obj.to_parquet(buffer)
                return ObjType.DataFrame.value + buffer.getvalue()

            if isinstance(obj, int):
                return ObjType.Integer.value + struct.pack("I", obj)

            return ObjType.General.value + cloudpickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

        @staticmethod
        def deserialize(payload: bytes) -> Any:
            obj_type = ObjType(payload[0:1])
            payload = payload[1:]

            if obj_type == ObjType.DataFrame:
                buffer = BytesIO(payload)
                return pd.read_parquet(buffer)

            if obj_type == ObjType.Integer:
                return struct.unpack("I", payload)[0]

            return cloudpickle.loads(payload)
