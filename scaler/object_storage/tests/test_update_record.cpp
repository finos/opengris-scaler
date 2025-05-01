
#include <capnp/message.h>
#include <capnp/serialize.h>
#include <gtest/gtest.h>

#include "../defs.h"
#include "../object_storage_server.h"
#include "protocol/object_storage.capnp.h"

using reqType  = ObjectRequestHeader::ObjectRequestType;
using respType = ObjectResponseHeader::ObjectResponseType;

std::string payload = "Hello, world!";

TEST(ObjectStorageTestSuite, TestSetObject) {
    scaler::object_storage::ObjectStorageServer server;
    scaler::object_storage::ObjectRequestHeader requestHeader;
    requestHeader.objectID      = {0, 1, 2, 3};
    requestHeader.payloadLength = payload.size();
    requestHeader.reqType       = reqType::SET_OBJECT;
    scaler::object_storage::ObjectResponseHeader responseHeader;
    std::vector<unsigned char> payload_vec(payload.begin(), payload.end());

    server.updateRecord(requestHeader, responseHeader, payload_vec);
    EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
    EXPECT_EQ(responseHeader.payloadLength, 0);
    EXPECT_EQ(responseHeader.respType, respType::SET_O_K);

    server.updateRecord(requestHeader, responseHeader, payload_vec);
    EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
    EXPECT_EQ(responseHeader.payloadLength, 0);
    EXPECT_EQ(responseHeader.respType, respType::SET_O_K_OVERRIDE);
}

TEST(ObjectStorageTestSuite, TestGetObjectWithObjectPresence) {
    scaler::object_storage::ObjectStorageServer server;
    scaler::object_storage::ObjectRequestHeader requestHeader;
    requestHeader.objectID      = {0, 1, 2, 3};
    requestHeader.payloadLength = payload.size();
    // prepare the object
    requestHeader.reqType = reqType::SET_OBJECT;
    scaler::object_storage::ObjectResponseHeader responseHeader;
    std::vector<unsigned char> payload_vec(payload.begin(), payload.end());
    server.updateRecord(requestHeader, responseHeader, payload_vec);

    // start tests
    requestHeader.reqType = reqType::GET_OBJECT;

    requestHeader.payloadLength = payload.size();  // All bytes
    server.updateRecord(requestHeader, responseHeader, {});
    EXPECT_EQ(responseHeader.payloadLength, requestHeader.payloadLength);

    requestHeader.payloadLength = 0;  // No byte
    server.updateRecord(requestHeader, responseHeader, {});
    EXPECT_EQ(responseHeader.payloadLength, requestHeader.payloadLength);

    requestHeader.payloadLength = 1;  // first byte
    server.updateRecord(requestHeader, responseHeader, {});
    EXPECT_EQ(responseHeader.payloadLength, requestHeader.payloadLength);

    requestHeader.payloadLength = -1;  // All bytes
    server.updateRecord(requestHeader, responseHeader, {});
    EXPECT_EQ(responseHeader.payloadLength, payload.size());
}

TEST(ObjectStorageTestSuite, TestDeleteObject) {
    scaler::object_storage::ObjectStorageServer server;
    scaler::object_storage::ObjectRequestHeader requestHeader;
    requestHeader.objectID      = {0, 1, 2, 3};
    requestHeader.payloadLength = payload.size();
    // prepare the object
    requestHeader.reqType = reqType::SET_OBJECT;
    scaler::object_storage::ObjectResponseHeader responseHeader;
    std::vector<unsigned char> payload_vec(payload.begin(), payload.end());
    server.updateRecord(requestHeader, responseHeader, payload_vec);

    // start test
    requestHeader.reqType = reqType::DELETE_OBJECT;
    server.updateRecord(requestHeader, responseHeader, {});
    EXPECT_EQ(responseHeader.respType, respType::DEL_O_K);

    server.updateRecord(requestHeader, responseHeader, {});
    EXPECT_EQ(responseHeader.respType, respType::DEL_NOT_EXISTS);
}

TEST(ObjectStorageTestSuite, TestEmptyObject) {
    scaler::object_storage::ObjectStorageServer server;
    scaler::object_storage::ObjectRequestHeader requestHeader;
    // prepare the object
    requestHeader.objectID      = {114, 514, 1919, 810};
    requestHeader.payloadLength = 0;
    requestHeader.reqType       = reqType::SET_OBJECT;
    scaler::object_storage::ObjectResponseHeader responseHeader;
    server.updateRecord(requestHeader, responseHeader, {});

    // Ver. 4 behavior
    // Get object len with 0 does not halt
    requestHeader.reqType = reqType::GET_OBJECT;
    server.updateRecord(requestHeader, responseHeader, {});
    EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
    EXPECT_EQ(responseHeader.payloadLength, 0);
    EXPECT_EQ(responseHeader.respType, respType::GET_O_K);

    // Get object len with bigger object length should return the actual object length
    server.updateRecord(requestHeader, responseHeader, {});
    EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
    EXPECT_EQ(responseHeader.payloadLength, 0);
    EXPECT_EQ(responseHeader.respType, respType::GET_O_K);
}
