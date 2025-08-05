#include "scaler/object_storage/message.h"

namespace scaler {
namespace object_storage {

kj::Array<const capnp::word> ObjectID::toBuffer() const
{
    capnp::MallocMessageBuilder returnMsg;
    auto objectIDRoot = returnMsg.initRoot<scaler::protocol::ObjectID>();

    objectIDRoot.setField0(value[0]);
    objectIDRoot.setField1(value[1]);
    objectIDRoot.setField2(value[2]);
    objectIDRoot.setField3(value[3]);

    return capnp::messageToFlatArray(returnMsg);
}

kj::Array<const capnp::word> ObjectRequestHeader::toBuffer() const
{
    capnp::MallocMessageBuilder returnMsg;
    auto reqRoot = returnMsg.initRoot<scaler::protocol::ObjectRequestHeader>();

    auto reqRootObjectID = reqRoot.initObjectID();
    reqRootObjectID.setField0(objectID[0]);
    reqRootObjectID.setField1(objectID[1]);
    reqRootObjectID.setField2(objectID[2]);
    reqRootObjectID.setField3(objectID[3]);

    reqRoot.setPayloadLength(payloadLength);
    reqRoot.setRequestID(requestID);
    reqRoot.setRequestType(requestType);

    return capnp::messageToFlatArray(returnMsg);
}

kj::Array<const capnp::word> ObjectResponseHeader::toBuffer() const
{
    capnp::MallocMessageBuilder returnMsg;
    auto respRoot = returnMsg.initRoot<scaler::protocol::ObjectResponseHeader>();

    auto respRootObjectID = respRoot.initObjectID();
    respRootObjectID.setField0(objectID[0]);
    respRootObjectID.setField1(objectID[1]);
    respRootObjectID.setField2(objectID[2]);
    respRootObjectID.setField3(objectID[3]);

    respRoot.setPayloadLength(payloadLength);
    respRoot.setResponseID(responseID);
    respRoot.setResponseType(responseType);

    return capnp::messageToFlatArray(returnMsg);
}

};  // namespace object_storage
};  // namespace scaler
