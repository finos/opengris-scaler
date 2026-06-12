@0xaf44f44ea94a4675;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("scaler::protocol");

using CommonType = import "common.capnp";
using Status = import "status.capnp";

struct Task {
    taskId @0 :Data;
    source @1 :Data;
    metadata @2 :Data;
    funcObjectId @3 :Data;
    functionArgs @4 :List(Argument);
    capabilities @5 :List(CommonType.TaskCapability);

    struct Argument {
        type @0 :ArgumentType;
        data @1 :Data;

        enum ArgumentType {
            task @0;
            objectID @1;
        }
    }
}

struct TaskCancel {
    struct TaskCancelFlags {
        force @0 :Bool;
    }

    taskId @0 :Data;
    flags @1 :TaskCancelFlags;
}

struct TaskLog {
    taskId @0 :Data;
    logType @1 :LogType;
    content @2 :Text;

    enum LogType {
        stdout @0;
        stderr @1;
    }
}

struct TaskResult {
    taskId @0 :Data;
    resultType @1 :CommonType.TaskResultType;
    metadata @2 :Data;
    results @3 :List(Data);
}

struct TaskCancelConfirm {
    taskId @0 :Data;
    cancelConfirmType @1 :CommonType.TaskCancelConfirmType;
}

struct GraphTask {
    taskId @0 :Data;
    source @1 :Data;
    targets @2 :List(Data);
    graph @3 :List(Task);
}

struct ClientHeartbeat {
    resource @0 :Status.Resource;
    latencyUS @1 :UInt32;
    actorIds @2 :List(Data);   # complete set of actors this client claims to own; always the
                               # full set, never a diff
}

struct ClientHeartbeatEcho {
    objectStorageAddress @0 :CommonType.ObjectStorageAddress;
}

struct WorkerHeartbeat {
    agent @0 :Status.Resource;
    rssFree @1 :UInt64;
    queueSize @2 :UInt32;
    queuedTasks @3 :UInt32;
    latencyUS @4 :UInt32;
    taskLock @5 :Bool;
    processors @6 :List(Status.ProcessorStatus);
    capabilities @7 :List(CommonType.TaskCapability);
    workerManagerID @8 :Data;
    actors @9 :List(Status.ActorHostStatus);
}

struct WorkerHeartbeatEcho {
    objectStorageAddress @0 :CommonType.ObjectStorageAddress;
}

struct WorkerManagerHeartbeat {
    maxTaskConcurrency @0 :UInt32;
    capabilities @1 :List(CommonType.TaskCapability);
    workerManagerID @2 :Data;
}

struct WorkerManagerHeartbeatEcho {
}

struct WorkerManagerCommand {
    struct DesiredTaskConcurrencyRequest {
        taskConcurrency @0 :UInt32;
        capabilities @1 :List(CommonType.TaskCapability);
    }

    setDesiredTaskConcurrencyRequests @0 :List(DesiredTaskConcurrencyRequest);
}

struct ObjectInstruction {
    instructionType @0 :ObjectInstructionType;
    objectUser @1 :Data;
    objectMetadata @2 :CommonType.ObjectMetadata;

    enum ObjectInstructionType {
        create @0;
        delete @1;
        clear @2;
    }
}

struct DisconnectRequest {
    worker @0 :Data;
}

struct DisconnectResponse {
    worker @0 :Data;
}

struct ClientDisconnect {
    disconnectType @0 :DisconnectType;

    enum DisconnectType {
        disconnect @0;
        shutdown @1;
    }
}

struct ClientShutdownResponse {
    accepted @0 :Bool;
}

struct StateClient {
}

struct StateObject {
}

struct StateBalanceAdvice {
    workerId @0 :Data;
    taskIds @1 :List(Data);
}

struct StateScheduler {
    binder @0 :Status.BinderStatus;
    scheduler @1 :Status.Resource;
    rssFree @2 :UInt64;
    clientManager @3 :Status.ClientManagerStatus;
    objectManager @4 :Status.ObjectManagerStatus;
    taskManager @5 :Status.TaskManagerStatus;
    workerManager @6 :Status.WorkerManagerStatus;
    scalingManager @7 :Status.ScalingManagerStatus;
}

struct StateWorker {
    workerId @0 :Data;
    state@1 :CommonType.WorkerState;
    capabilities @2 :List(CommonType.TaskCapability);
}

struct StateTask {
    taskId @0 :Data;
    functionName @1 :Data;
    state @2 :CommonType.TaskState;
    worker @3 :Data;
    capabilities @4 :List(CommonType.TaskCapability);
    metadata @5 :Data;
}

struct StateGraphTask {
    enum NodeTaskType {
        normal @0;
        target @1;
    }

    graphTaskId @0 :Data;
    taskId @1 :Data;
    nodeTaskType @2 :NodeTaskType;
    parentTaskIds @3 :List(Data);
}

struct ProcessorInitialized {
}

struct InformationRequest {
    request @0 :Data;
}

struct InformationResponse {
    response @0 :Data;
}

# Convention for every actor message:
#   actorId: client-generated, globally unique, never reused across incarnations
#   source:  identity of the owning client; sender on client->worker messages, destination on
#            worker->client messages

# Declares "this actor should exist". Idempotent per actorId: re-sending is answered with the
# actor's current ActorStateUpdate, never a duplicate actor.
struct ActorCreate {
    actorId @0 :Data;
    source @1 :Data;
    classObjectId @2 :Data;    # object id of the serialized actor class
    constructorArguments @3 :CommonType.ActorArguments;
    capabilities @4 :List(CommonType.TaskCapability);
}

# Declares "this actor should not exist". Idempotent and escalatable (graceful then kill).
struct ActorDestroy {
    actorId @0 :Data;
    source @1 :Data;
    mode @2 :Mode;

    enum Mode {
        graceful @0;   # let the actor wind down cooperatively, then exit
        kill @1;       # terminate the actor process immediately
    }
}

# Declares the full current state of one actor; sent on every transition and re-sendable at
# any time. Receivers treat it as absolute truth, so it is safe to duplicate and safe to lose
# (heartbeat reconciliation re-derives it).
struct ActorStateUpdate {
    actorId @0 :Data;
    source @1 :Data;
    workerId @2 :Data;                  # assigned worker; empty while pending
    state @3 :CommonType.ActorState;
    deathInfo @4 :DeathInfo;            # only meaningful when state == dead

    struct DeathInfo {
        reason @0 :Reason;
        error @1 :CommonType.ActorError;   # populated for constructorFailed/actorCrashed

        enum Reason {
            destroyed @0;           # client-requested destroy completed
            constructorFailed @1;   # actor constructor raised
            actorCrashed @2;        # actor process exited unexpectedly
            workerDied @3;          # hosting worker lost (heartbeat timeout)
            clientDisconnected @4;  # owning client lost; scheduler reclaimed the actor
            placementFailed @5;     # no worker satisfies the requested capabilities
            unknownActor @6;        # actorId unknown to the scheduler
        }
    }
}

# The actor data plane. payload is opaque to the scheduler and the worker agent; it is routed
# by actorId (client->worker) and source (worker->client).
struct ActorMessage {
    actorId @0 :Data;
    source @1 :Data;
    payload @2 :Data;
}

struct StateActor {
    actorId @0 :Data;
    source @1 :Data;
    workerId @2 :Data;
    className @3 :Data;
    state @4 :CommonType.ActorState;
    capabilities @5 :List(CommonType.TaskCapability);
}

struct Message {
    union {
        task @0 :Task;
        taskCancel @1 :TaskCancel;
        taskCancelConfirm @2 :TaskCancelConfirm;
        taskResult @3 :TaskResult;
        taskLog @4 :TaskLog;

        graphTask @5 :GraphTask;

        objectInstruction @6 :ObjectInstruction;

        clientHeartbeat @7 :ClientHeartbeat;
        clientHeartbeatEcho @8 :ClientHeartbeatEcho;

        workerHeartbeat @9 :WorkerHeartbeat;
        workerHeartbeatEcho @10 :WorkerHeartbeatEcho;

        disconnectRequest @11 :DisconnectRequest;
        disconnectResponse @12 :DisconnectResponse;

        stateClient @13 :StateClient;
        stateObject @14 :StateObject;
        stateBalanceAdvice @15 :StateBalanceAdvice;
        stateScheduler @16 :StateScheduler;
        stateWorker @17 :StateWorker;
        stateTask @18 :StateTask;
        stateGraphTask @19 :StateGraphTask;

        clientDisconnect @20 :ClientDisconnect;
        clientShutdownResponse @21 :ClientShutdownResponse;

        processorInitialized @22 :ProcessorInitialized;

        informationRequest @23 :InformationRequest;
        informationResponse @24 :InformationResponse;

        workerManagerHeartbeat @25 :WorkerManagerHeartbeat;
        workerManagerHeartbeatEcho @26 :WorkerManagerHeartbeatEcho;
        workerManagerCommand @27 :WorkerManagerCommand;

        actorCreate @28 :ActorCreate;
        actorDestroy @29 :ActorDestroy;
        actorStateUpdate @30 :ActorStateUpdate;
        actorMessage @31 :ActorMessage;
        stateActor @32 :StateActor;
    }
}
