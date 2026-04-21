import struct
from typing import Dict

import bidict

from scaler.protocol import capnp
from scaler.protocol.capnp import ObjectID as CapnpObjectID
from scaler.utility.identifiers import ObjectID as ScalerObjectID

OBJECT_ID_FORMAT = "!QQQQ"


def to_capnp_object_id(object_id: ScalerObjectID):
    field0, field1, field2, field3 = struct.unpack(OBJECT_ID_FORMAT, object_id)
    return capnp.ObjectID(field0=field0, field1=field1, field2=field2, field3=field3)


def from_capnp_object_id(capnp_object_id: CapnpObjectID) -> ScalerObjectID:
    return ScalerObjectID(
        struct.pack(
            OBJECT_ID_FORMAT,
            capnp_object_id.field0,
            capnp_object_id.field1,
            capnp_object_id.field2,
            capnp_object_id.field3,
        )
    )


def capabilities_to_dict(capabilities) -> Dict[str, int]:
    if isinstance(capabilities, dict):
        return dict(capabilities)

    return {capability.name: capability.value for capability in capabilities}


PROTOCOL: bidict.bidict[str, type] = bidict.bidict(
    {
        "task": capnp.Task,
        "taskCancel": capnp.TaskCancel,
        "taskCancelConfirm": capnp.TaskCancelConfirm,
        "taskResult": capnp.TaskResult,
        "taskLog": capnp.TaskLog,
        "graphTask": capnp.GraphTask,
        "objectInstruction": capnp.ObjectInstruction,
        "clientHeartbeat": capnp.ClientHeartbeat,
        "clientHeartbeatEcho": capnp.ClientHeartbeatEcho,
        "workerHeartbeat": capnp.WorkerHeartbeat,
        "workerHeartbeatEcho": capnp.WorkerHeartbeatEcho,
        "workerManagerHeartbeat": capnp.WorkerManagerHeartbeat,
        "workerManagerHeartbeatEcho": capnp.WorkerManagerHeartbeatEcho,
        "workerManagerCommand": capnp.WorkerManagerCommand,
        "workerManagerCommandResponse": capnp.WorkerManagerCommandResponse,
        "disconnectRequest": capnp.DisconnectRequest,
        "disconnectResponse": capnp.DisconnectResponse,
        "stateClient": capnp.StateClient,
        "stateObject": capnp.StateObject,
        "stateBalanceAdvice": capnp.StateBalanceAdvice,
        "stateScheduler": capnp.StateScheduler,
        "stateWorker": capnp.StateWorker,
        "stateTask": capnp.StateTask,
        "stateGraphTask": capnp.StateGraphTask,
        "clientDisconnect": capnp.ClientDisconnect,
        "clientShutdownResponse": capnp.ClientShutdownResponse,
        "processorInitialized": capnp.ProcessorInitialized,
        "informationRequest": capnp.InformationRequest,
        "informationResponse": capnp.InformationResponse,
    }
)
