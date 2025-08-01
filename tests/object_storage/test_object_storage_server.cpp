#include <gtest/gtest.h>

#include <boost/asio.hpp>
#include <thread>

#include "scaler/object_storage/object_storage_server.h"

// using boost::asio::awaitable;
using boost::asio::buffer;
using boost::asio::ip::tcp;

using namespace scaler::object_storage;

using ObjectRequestType  = scaler::protocol::ObjectRequestHeader::ObjectRequestType;
using ObjectResponseType = scaler::protocol::ObjectResponseHeader::ObjectResponseType;

class ObjectStorageServerTest: public ::testing::Test {
protected:
    static constexpr std::string SERVER_HOST = "127.0.0.1";

    class ObjectStorageClient {
    public:
        ObjectStorageClient(boost::asio::io_context& ioContext, std::string serverPort): socket(ioContext) {
            tcp::resolver resolver(ioContext);
            boost::asio::connect(socket, resolver.resolve(SERVER_HOST, serverPort));
        }

        ~ObjectStorageClient() {
            boost::system::error_code ec;
            socket.shutdown(tcp::socket::shutdown_both, ec);
            socket.close(ec);
        }

        ObjectStorageClient(const ObjectStorageClient&)            = delete;
        ObjectStorageClient& operator=(const ObjectStorageClient&) = delete;

        void writeRequest(const ObjectRequestHeader& header, const std::optional<ObjectPayload>& payload) {
            auto buf = header.toBuffer();

            boost::asio::write(socket, boost::asio::buffer(buf.asBytes().begin(), buf.asBytes().size()));

            if (payload) {
                boost::asio::write(socket, boost::asio::buffer(*payload));
            }
        }

        void readResponse(ObjectResponseHeader& header, std::optional<ObjectPayload>& payload) {
            std::array<uint64_t, CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE> buf;
            boost::asio::read(socket, boost::asio::buffer(buf.data(), CAPNP_HEADER_SIZE));

            header = ObjectResponseHeader::fromBuffer(buf);

            if (header.payloadLength > 0) {
                payload.emplace(header.payloadLength);
                boost::asio::read(socket, boost::asio::buffer(*payload));
            } else {
                payload.reset();
            }
        }

    private:
        tcp::socket socket;
    };

    std::unique_ptr<ObjectStorageServer> server;

    std::string serverPort;
    std::thread serverThread;

    boost::asio::io_context ioContext;

    void SetUp() override {
        server = std::make_unique<ObjectStorageServer>();

        serverPort = std::to_string(getAvailableTCPPort());

        serverThread = std::thread([this] { server->run(SERVER_HOST, serverPort); });

        server->waitUntilReady();
    }

    void TearDown() override {
        server->shutdown();
        if (serverThread.joinable()) {
            serverThread.join();
        }
        server.reset();
    }

    std::unique_ptr<ObjectStorageClient> getClient() {
        return std::make_unique<ObjectStorageClient>(ioContext, serverPort);
    }
};

const ObjectPayload payload {'H', 'e', 'l', 'l', 'o'};

TEST_F(ObjectStorageServerTest, TestSetObject) {
    ObjectResponseHeader responseHeader;
    std::optional<ObjectPayload> responsePayload;

    auto client = getClient();

    ObjectRequestHeader requestHeader {
        .objectID      = {0, 1, 2, 3},
        .payloadLength = payload.size(),
        .requestID     = 42,
        .requestType   = ObjectRequestType::SET_OBJECT,
    };

    client->writeRequest(requestHeader, {payload});
    client->readResponse(responseHeader, responsePayload);

    EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
    EXPECT_EQ(responseHeader.payloadLength, 0);
    EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    EXPECT_FALSE(responsePayload.has_value());
}

TEST_F(ObjectStorageServerTest, TestGetObject) {
    ObjectResponseHeader responseHeader;
    std::optional<ObjectPayload> responsePayload;
    uint64_t requestID = 12;

    auto client = getClient();

    // Set the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = payload.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, {payload});

        client->readResponse(responseHeader, responsePayload);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
        EXPECT_FALSE(responsePayload.has_value());
    }

    // Get the full object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        requestHeader.payloadLength = UINT64_MAX;
        requestHeader.requestType   = ObjectRequestType::GET_OBJECT,

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
        EXPECT_EQ(responseHeader.payloadLength, payload.size());
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
        EXPECT_EQ(*responsePayload, payload);
    }

    // Get the first byte of the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = 1,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.payloadLength, 1);
        EXPECT_EQ(responsePayload->size(), 1);
        EXPECT_EQ(responsePayload->front(), payload.front());
    }
}

TEST_F(ObjectStorageServerTest, TestDeleteObject) {
    ObjectResponseHeader responseHeader;
    std::optional<ObjectPayload> responsePayload;
    uint64_t requestID = 16;

    auto client = getClient();

    // Try to delete a non existing object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = 0,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DELETE_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);

        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
        EXPECT_EQ(responseHeader.payloadLength, 0);
        EXPECT_EQ(responseHeader.responseID, requestHeader.requestID);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DEL_NOT_EXISTS);
        EXPECT_FALSE(responsePayload.has_value());
    }

    // Set the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = payload.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, {payload});
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // Delete the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = 0,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DELETE_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
        EXPECT_EQ(responseHeader.payloadLength, 0);
        EXPECT_EQ(responseHeader.responseID, requestHeader.requestID);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DEL_O_K);
        EXPECT_FALSE(responsePayload.has_value());

        // Delete again should fail

        requestHeader.requestID = requestID++;

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DEL_NOT_EXISTS);
    }
}

TEST_F(ObjectStorageServerTest, TestDuplicateObject) {
    ObjectResponseHeader responseHeader;
    std::optional<ObjectPayload> responsePayload;
    uint64_t requestID = 655;

    auto client = getClient();

    ObjectID originalObjectID {0, 6, 3, 91};
    ObjectID newObjectID {0, 91, 3, 6};

    // Set the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = originalObjectID,
            .payloadLength = payload.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, {payload});

        client->readResponse(responseHeader, responsePayload);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // Duplicate the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = newObjectID,
            .payloadLength = ObjectID::bufferSize(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DUPLICATE_OBJECT_I_D,
        };

        auto originalObjectIDBuffer = originalObjectID.toBuffer();
        ObjectPayload originalObjectIDPayload {
            reinterpret_cast<const uint8_t*>(originalObjectIDBuffer.begin()),
            reinterpret_cast<const uint8_t*>(originalObjectIDBuffer.end())};

        client->writeRequest(requestHeader, {originalObjectIDPayload});

        client->readResponse(responseHeader, responsePayload);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DUPLICATE_O_K);
    }

    // Get the duplicated object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = newObjectID,
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client->writeRequest(requestHeader, {});

        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
        EXPECT_TRUE(responsePayload.has_value());
        EXPECT_EQ(*responsePayload, payload);
    }
}

TEST_F(ObjectStorageServerTest, TestEmptyObject) {
    ObjectResponseHeader responseHeader;
    std::optional<ObjectPayload> responsePayload;
    uint64_t requestID = 265;

    auto client = getClient();

    // Set a 0 byte object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {114, 514, 1919, 810},
            .payloadLength = 0,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        ObjectPayload emptyPayload {};

        client->writeRequest(requestHeader, {emptyPayload});
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.objectID, requestHeader.objectID);
        EXPECT_EQ(responseHeader.payloadLength, 0);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // Get the 0 byte object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = {114, 514, 1919, 810},
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client->writeRequest(requestHeader, std::nullopt);
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.payloadLength, 0);
        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
    }
}

TEST_F(ObjectStorageServerTest, TestRequestBlocking) {
    ObjectResponseHeader responseHeader;
    std::optional<ObjectPayload> responsePayload;
    uint64_t requestID = 42;

    auto client1 = getClient();
    auto client2 = getClient();
    auto client3 = getClient();

    ObjectID originalObjectID {0, 0, 0, 1};
    ObjectID duplicatedObjectID {0, 0, 0, 2};

    // Client 1 sends a blocking get request for the not yet duplicated object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = duplicatedObjectID,
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client1->writeRequest(requestHeader, {});
    }

    // Client 3 duplicates the not yet submitted original object's data
    {
        ObjectRequestHeader requestHeader {
            .objectID      = duplicatedObjectID,
            .payloadLength = ObjectID::bufferSize(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::DUPLICATE_OBJECT_I_D,
        };

        auto objectIDBuffer = originalObjectID.toBuffer();
        ObjectPayload objectIDPayload {
            reinterpret_cast<const uint8_t*>(objectIDBuffer.begin()),
            reinterpret_cast<const uint8_t*>(objectIDBuffer.end())};

        client3->writeRequest(requestHeader, {objectIDPayload});
    }

    // Client 2 sends the original object's data
    {
        ObjectRequestHeader requestHeader {
            .objectID      = originalObjectID,
            .payloadLength = payload.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client2->writeRequest(requestHeader, {payload});
        client2->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // Client 3 receives the duplicate OK response
    {
        client3->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::DUPLICATE_O_K);
    }

    // Client 1 receives the duplicated object's data
    {
        client1->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
        EXPECT_EQ(*responsePayload, payload);
    }
}
