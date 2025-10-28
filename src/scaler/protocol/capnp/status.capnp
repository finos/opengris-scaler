@0xa4dfa1212ad2d0f0;

struct Resource {
    cpu @0 :UInt16;   # 99.2% will be represented as 992 as integer
    rss @1 :UInt64;  # 32bit is capped to 4GB, so use 64bit to represent
}

struct ObjectManagerStatus {
    numberOfObjects @0 :UInt32;
}

struct ClientManagerStatus {
    clientToNumOfTask @0 :List(Pair);

    struct Pair {
        client @0 :Data;
        numTask @1 :UInt32;
    }
}

struct TaskManagerStatus {
    stateToCount @0 :List(Pair);

    struct Pair {
        state @0 :UInt8;
        count @1 :UInt32;
    }
}

struct ProcessorStatus {
    pid @0 :UInt32;
    initialized @1 :Bool;
    hasTask @2 :Bool;
    suspended @3 :Bool;
    resource @4 :Resource;
}

struct WorkerStatus {
    workerId @0 :Data;
    agent @1 :Resource;
    rssFree @2 :UInt64;
    free @3 :UInt32;
    sent @4 :UInt32;
    queued @5 :UInt32;
    suspended @6: UInt8;
    lagUS @7 :UInt64;
    lastS @8 :UInt8;
    itl @9 :Text;
    processorStatuses @10 :List(ProcessorStatus);
}

struct WorkerManagerStatus {
    workers @0 :List(WorkerStatus);
}

struct ScalingManagerStatus {
    workerGroups @0 :List(Pair);

    struct Pair {
        workerGroupID @0 :Data;
        workerIDs @1 :List(Data);
    }
}

struct BinderStatus {
    received @0 :List(Pair);
    sent @1 :List(Pair);

    struct Pair {
        client @0 :Text;
        number @1 :UInt32;
    }
}
