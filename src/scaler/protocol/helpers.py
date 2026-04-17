import struct
from typing import Dict, Optional

import bidict

from scaler.protocol import capnp
from scaler.protocol.capnp import ObjectID as CapnpObjectID
from scaler.protocol.capnp import ScalingManagerStatus
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


def build_scaling_manager_status(
    managed_workers: Dict[bytes, list], worker_manager_details: Optional[list] = None
) -> ScalingManagerStatus:
    details = worker_manager_details or []
    return capnp.ScalingManagerStatus(
        managedWorkers=[
            capnp.ScalingManagerStatus.Pair(
                workerManagerID=worker_manager_id, workerIDs=[bytes(worker_id) for worker_id in worker_ids]
            )
            for worker_manager_id, worker_ids in managed_workers.items()
        ],
        workerManagerDetails=[
            capnp.ScalingManagerStatus.WorkerManagerDetail(
                workerManagerID=d["worker_manager_id"],
                identity=d["identity"],
                lastSeenS=d["last_seen_s"],
                maxTaskConcurrency=d["max_task_concurrency"],
                capabilities=d.get("capabilities", ""),
                pendingWorkers=d.get("pending_workers", 0),
            )
            for d in details
        ],
    )


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
