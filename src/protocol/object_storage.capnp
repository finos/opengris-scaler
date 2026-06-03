@0xc2a14174aa42a12a;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("scaler::protocol");

struct ObjectRequestHeader {
    objectID @0: ObjectID; # 32 bytes
    payloadLength @1: UInt64; # 8 bytes
    requestID @2: UInt64; # 8 bytes
    requestType @3: ObjectRequestType; # 2 bytes

    enum ObjectRequestType {
        # Set or override an object to the message's payload.
        # Overrides the object's content if it already exists
        # Always immediately answers with a setOK message.
        setObject @0;

        # Get an object's content.
        # If the object does not exist, delays the getOk response until the object is created.
        getObject @1;

        # Remove the object.
        deleteObject @2;

        # Creates the provided object ID by linking it to the content of the object ID provided in payload.
        # Overrides the object content if the new object ID already exists.
        # If the referenced object does not exist, delays the duplicateOK response until the original object is created.
        duplicateObjectID @3;

        # Request the server to give back internal information, result is returned as payload.
        # schema: three uint64_t tuple (number of ids, number of objects (hashes), total actual object size in bytes)
        infoGetTotal @4;
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
    responseID @2: UInt64; # 8 bytes
    responseType @3: ObjectResponseType;

    enum ObjectResponseType {
        setOK @0;
        getOK @1;
        delOK @2;
        delNotExists @3;
        duplicateOK @4;
        infoGetTotalOK @5;
    }
}
