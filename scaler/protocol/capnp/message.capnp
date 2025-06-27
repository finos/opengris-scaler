@0xaf44f44ea94a4675;

using CommonType = import "common.capnp";
using Status = import "status.capnp";

struct Task {
    taskId @0 :Data;
    source @1 :Data;
    metadata @2 :Data;
    funcObjectId @3 :Data;
    functionArgs @4 :List(Argument);

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
        retrieveTaskObject @1 :Bool;
    }

    taskId @0 :Data;
    flags @1 :TaskCancelFlags;
}

struct TaskResult {
    taskId @0 :Data;
    status @1 :CommonType.TaskStatus;
    metadata @2 :Data;
    results @3 :List(Data);
}

struct GraphTask {
    taskId @0 :Data;
    source @1 :Data;
    targets @2 :List(Data);
    graph @3 :List(Task);
}

struct GraphTaskCancel {
    taskId @0 :Data;
}

struct ClientHeartbeat {
    resource @0 :Status.Resource;
    latencyUS @1 :UInt32;
}

struct ClientHeartbeatEcho {
    objectStorageAddress @0 :CommonType.ObjectStorageAddress;
}

struct WorkerHeartbeat {
    agent @0 :Status.Resource;
    rssFree @1 :UInt64;
    queuedTasks @2 :UInt32;
    latencyUS @3 :UInt32;
    taskLock @4 :Bool;
    processors @5 :List(Status.ProcessorStatus);
}

struct WorkerHeartbeatEcho {
    objectStorageAddress @0 :CommonType.ObjectStorageAddress;
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
}

struct StateWorker {
    workerId @0 :Data;
    message @1 :Data;
}

struct StateTask {
    taskId @0 :Data;
    functionName @1 :Data;
    status @2 :CommonType.TaskStatus;
    worker @3 :Data;
    metadata @4 :Data;
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


struct Message {
    union {
        task @0 :Task;
        taskCancel @1 :TaskCancel;
        taskResult @2 :TaskResult;

        graphTask @3 :GraphTask;
        graphTaskCancel @4 :GraphTaskCancel;

        objectInstruction @5 :ObjectInstruction;

        clientHeartbeat @6 :ClientHeartbeat;
        clientHeartbeatEcho @7 :ClientHeartbeatEcho;

        workerHeartbeat @8 :WorkerHeartbeat;
        workerHeartbeatEcho @9 :WorkerHeartbeatEcho;

        disconnectRequest @10 :DisconnectRequest;
        disconnectResponse @11 :DisconnectResponse;

        stateClient @12 :StateClient;
        stateObject @13 :StateObject;
        stateBalanceAdvice @14 :StateBalanceAdvice;
        stateScheduler @15 :StateScheduler;
        stateWorker @16 :StateWorker;
        stateTask @17 :StateTask;
        stateGraphTask @18 :StateGraphTask;

        clientDisconnect @19 :ClientDisconnect;
        clientShutdownResponse @20 :ClientShutdownResponse;

        processorInitialized @21 :ProcessorInitialized;
    }
}
