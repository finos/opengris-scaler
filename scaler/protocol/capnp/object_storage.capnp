@0xc2a14174aa42a12a;

struct ObjectRequestHeader {
    objectID @0: ObjectID; # 32 bytes
    payloadLength @1: UInt64; # 8 bytes
    requestType @2: ObjectRequestType; # 2 bytes

    enum ObjectRequestType {
        setObject @0;       # send object to object storage, in the future, we might provide expiration time
        getObject @1;       # get object when it's ready, it will be similar like subscribe
        deleteObject @2;
    }
}

struct ObjectID {
    field0 @0: UInt64;
    field1 @1: UInt64;
    field2 @2: UInt64;
    field3 @3: UInt64;
}

struct ObjectResponseHeader {
    objectID @0: ObjectID;
    payloadLength @1: UInt64;
    responseType @2: ObjectResponseType;

    enum ObjectResponseType {
        setOK @0;
        setOKOverride @1;
        getOK @2;             # if object not exists, it will hang
        delOK @3;
        delNotExists @4;
    }
}
