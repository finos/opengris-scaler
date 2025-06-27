@0xf57f79ac88fab620;

enum TaskStatus {
    # task is accepted by scheduler, but will have below status
    success @0;     # if submit and task is done and get result
    failed @1;      # if submit and task is failed on worker
    canceled @2;    # if submit and task is canceled
    notFound @3;    # if submit and task is not found in scheduler
    workerDied @4;  # if submit and worker died (only happened when scheduler keep_task=False)
    noWorker @5;    # if submit and scheduler is full (not implemented yet)

    # below are only used for monitoring channel, not sent to client
    inactive @6;    # task is scheduled but not allocate to worker
    running @7;     # task is running in worker
    canceling @8;   # task is canceling (can be in Inactive or Running state)
}

struct ObjectMetadata {
    objectIds @0 :List(Data);
    objectTypes @1 :List(ObjectContentType);
    objectNames @2 :List(Data);

    enum ObjectContentType {
        serializer @0;
        object @1;
    }
}

struct ObjectStorageAddress {
    host @0 :Text;
    port @1 :UInt16;
}
