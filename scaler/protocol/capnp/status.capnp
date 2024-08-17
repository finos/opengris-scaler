@0xa4dfa1212ad2d0f0;

struct Resource {
    cpu @0 :UInt16;   # 99.2% will be represented as 992 as integer
    rss @1 :UInt64;  # 32bit is capped to 4GB, so use 64bit to represent
}

struct ObjectManagerStatus {
    numberOfObjects @0 :UInt32;
    objectMemory @1 :UInt64;
}

struct ClientManagerStatus {
    clientToNumOfTask @0 :List(Pair);

    struct Pair {
        client @0 :Data;
        numTask @1 :UInt32;
    }
}

struct TaskManagerStatus {
    unassigned @0 :UInt32;
    running @1 :UInt32;
    success @2 :UInt32;
    failed @3 :UInt32;
    canceled @4 :UInt32;
    notFound @5 :UInt32;
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

struct BinderStatus {
    received @0 :List(Pair);
    sent @1 :List(Pair);

    struct Pair {
        client @0 :Text;
        number @1 :UInt32;
    }
}
