#include <gtest/gtest.h>

#include <cstring>
#include <filesystem>
#include <string>
#include <thread>

#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/simple_interface.h"
#include "scaler/object_storage/object_storage_server.h"

using namespace scaler::object_storage;
using namespace scaler::ymq;

using ObjectRequestType  = scaler::protocol::ObjectRequestHeader::ObjectRequestType;
using ObjectResponseType = scaler::protocol::ObjectResponseHeader::ObjectResponseType;

// This client class is used to connect to and interact with the server.
class ObjectStorageClient {
public:
    ObjectStorageClient(
        std::shared_ptr<IOContext> ioContext, const std::string& serverHost, const std::string serverPort)
        : ioContext(ioContext)
    {
        static int id = 0;
        ioSocket =
            syncCreateSocket(*ioContext.get(), IOSocketType::Connector, "ObjectStorageClient" + std::to_string(id++));
        const std::string address = "tcp://" + serverHost + ':' + serverPort;
        syncConnectSocket(ioSocket, address);
    }

    ~ObjectStorageClient() { ioContext->removeIOSocket(ioSocket); }

    ObjectStorageClient(const ObjectStorageClient&)            = delete;
    ObjectStorageClient& operator=(const ObjectStorageClient&) = delete;

    void writeYMQMessage(Message message)
    {
        auto error = syncSendMessage(ioSocket, std::move(message));
        ASSERT_TRUE(!error);
    }

    auto readYMQMessage() { return syncRecvMessage(ioSocket); }

    void writeRequest(const ObjectRequestHeader& header, const std::optional<ObjectPayload>& payload)
    {
        auto buf = header.toBuffer();

        Message ymqHeader {};
        ymqHeader.payload = Bytes((char*)buf.asBytes().begin(), buf.asBytes().size());
        writeYMQMessage(std::move(ymqHeader));

        if (payload) {
            Message ymqPayload {};
            ymqPayload.payload = *payload;
            writeYMQMessage(std::move(ymqPayload));
        }
    }

    void readResponse(ObjectResponseHeader& header, std::optional<ObjectPayload>& payload)
    {
        std::array<uint64_t, CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE> buf {};
        auto result = syncRecvMessage(ioSocket);
        ASSERT_TRUE(result.has_value());

        memcpy(buf.begin(), result->payload.data(), CAPNP_HEADER_SIZE);
        ASSERT_EQ(result->payload.size(), CAPNP_HEADER_SIZE);
        header = ObjectResponseHeader::fromBuffer(buf);

        if (header.payloadLength > 0) {
            auto result2 = syncRecvMessage(ioSocket);
            ASSERT_TRUE(result2.has_value());
            payload.emplace(result2->payload);
        } else {
            payload.reset();
        }
    }

private:
    std::shared_ptr<IOContext> ioContext;
    std::shared_ptr<IOSocket> ioSocket;
};

// This test fixture is for functional testing of the server's core features.
class ObjectStorageServerTest: public ::testing::Test {
protected:
    static constexpr std::string SERVER_HOST = "127.0.0.1";
    std::unique_ptr<ObjectStorageServer> server;

    std::string serverPort;
    std::thread serverThread;

    inline static std::shared_ptr<IOContext> ioContext;
    static void SetUpTestSuite() { ioContext = std::make_shared<IOContext>(); }
    static void TearDownTestSuite() { ioContext.reset(); }

    void SetUp() override
    {
        server = std::make_unique<ObjectStorageServer>();

        serverPort = std::to_string(getAvailableTCPPort());

        serverThread = std::thread([this] {
            server->run(SERVER_HOST, serverPort, "ObjectStorageServerTest", "INFO", "%(levelname)s: %(message)s");
        });

        server->waitUntilReady();
    }

    void TearDown() override
    {
        server->shutdown();
        if (serverThread.joinable()) {
            serverThread.join();
        }
        server.reset();
    }

    std::unique_ptr<ObjectStorageClient> getClient()
    {
        return std::make_unique<ObjectStorageClient>(ioContext, SERVER_HOST, serverPort);
    }
};

const ObjectPayload payload {std::string("Hello")};

TEST_F(ObjectStorageServerTest, TestSetObject)
{
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

TEST_F(ObjectStorageServerTest, TestGetObject)
{
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
        requestHeader.requestType   = ObjectRequestType::GET_OBJECT;

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
        EXPECT_EQ(*responsePayload->data(), *payload.data());
    }
}

TEST_F(ObjectStorageServerTest, TestDeleteObject)
{
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

TEST_F(ObjectStorageServerTest, TestDuplicateObject)
{
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
            reinterpret_cast<char*>(const_cast<unsigned char*>(originalObjectIDBuffer.asBytes().begin())),
            originalObjectIDBuffer.asBytes().size()};

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

TEST_F(ObjectStorageServerTest, TestEmptyObject)
{
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

TEST_F(ObjectStorageServerTest, TestRequestBlocking)
{
    ObjectResponseHeader responseHeader;
    std::optional<ObjectPayload> responsePayload;
    uint64_t requestID = 42;

    auto client1 = getClient();
    auto client2 = getClient();
    auto client3 = getClient();

    ObjectID originalObjectID {0, 0, 0, 1};
    ObjectID duplicatedObjectID {0, 0, 0, 2};

    // Client 1 sends a blocking GET request for the not yet duplicated object
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
            reinterpret_cast<char*>(const_cast<unsigned char*>(objectIDBuffer.asBytes().begin())),
            objectIDBuffer.asBytes().size()};

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

TEST_F(ObjectStorageServerTest, TestClientDisconnect)
{
    ObjectResponseHeader responseHeader;
    std::optional<ObjectPayload> responsePayload;
    uint64_t requestID = 100;

    auto client1 = getClient();
    auto client2 = getClient();
    auto client3 = getClient();

    ObjectID objectID {0, 1, 2, 3};

    // Client 1 tries to get the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = objectID,
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client1->writeRequest(requestHeader, std::nullopt);
    }

    // Client 2 tries to get the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = objectID,
            .payloadLength = UINT64_MAX,
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::GET_OBJECT,
        };

        client2->writeRequest(requestHeader, std::nullopt);
    }

    // Client 1 disconnects
    client1.reset();

    // Client 3 sets the object
    {
        ObjectRequestHeader requestHeader {
            .objectID      = objectID,
            .payloadLength = payload.size(),
            .requestID     = requestID++,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client3->writeRequest(requestHeader, {payload});
        client3->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }

    // Client 2 receives the object
    {
        client2->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::GET_O_K);
        EXPECT_EQ(*responsePayload, payload);
    }
}

TEST_F(ObjectStorageServerTest, TestMalformedHeader)
{
    ObjectResponseHeader responseHeader;
    std::optional<ObjectPayload> responsePayload;

    // Server should disconnect when it receives a garbage header
    {
        auto client = getClient();

        std::array<uint8_t, CAPNP_HEADER_SIZE> malformedHeader;
        malformedHeader.fill(0xAA);

        Message message;
        message.payload = Bytes((char*)malformedHeader.begin(), malformedHeader.size());
        client->writeYMQMessage(std::move(message));

        // Server should disconnect before or while we are reading the response
        auto result = client->readYMQMessage();
        EXPECT_TRUE(!result);
        EXPECT_EQ(result.error()._errorCode, Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
    }

    // Server must still answers to requests from other clients
    {
        auto client = getClient();

        ObjectRequestHeader requestHeader {
            .objectID      = {0, 1, 2, 3},
            .payloadLength = payload.size(),
            .requestID     = 42,
            .requestType   = ObjectRequestType::SET_OBJECT,
        };

        client->writeRequest(requestHeader, {payload});
        client->readResponse(responseHeader, responsePayload);

        EXPECT_EQ(responseHeader.responseType, ObjectResponseType::SET_O_K);
    }
}

// This test fixture is specifically for verifying server logging behavior.
class ObjectStorageLoggingTest: public ::testing::Test {
protected:
    static constexpr std::string SERVER_HOST = "127.0.0.1";
    std::filesystem::path log_filepath;

    std::unique_ptr<scaler::object_storage::ObjectStorageServer> server;
    std::string serverPort;
    std::thread serverThread;
    std::shared_ptr<IOContext> ioContext;

    void SetUp() override
    {
        ioContext    = std::make_shared<IOContext>();
        log_filepath = std::filesystem::temp_directory_path();

        std::string unique_filename = "server_log_" + std::to_string(getpid()) + "_" +
                                      std::to_string(std::hash<std::thread::id> {}(std::this_thread::get_id())) +
                                      ".txt";
        log_filepath /= unique_filename;

        server     = std::make_unique<scaler::object_storage::ObjectStorageServer>();
        serverPort = std::to_string(getAvailableTCPPort());

        serverThread = std::thread([this] {
            server->run(
                SERVER_HOST,
                serverPort,
                "ObjectStorageLoggingTest",
                "INFO",
                "%(levelname)s: %(message)s",
                {log_filepath.string()});
        });
        server->waitUntilReady();
    }

    void TearDown() override
    {
        server->shutdown();
        if (serverThread.joinable()) {
            serverThread.join();
        }
        server.reset();
        // Delete the log file after the test is complete.
        std::error_code ec;
        std::filesystem::remove(log_filepath, ec);
    }

    // Reads the content of the log file for verification.
    std::string readLogFile()
    {
        std::ifstream file(log_filepath);
        if (!file.is_open())
            return "";
        std::stringstream buffer;
        buffer << file.rdbuf();
        return buffer.str();
    }
};

TEST_F(ObjectStorageLoggingTest, TestServerLogsStartToFile)
{
    {
        // Use the functional client to connect and then disconnect.
        ObjectStorageClient client(ioContext, SERVER_HOST, serverPort);
    }
    // Give the server time to process the disconnection and write the log.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    std::string log_content = readLogFile();
    // Verify that the disconnection message is present in the log file.
    EXPECT_NE(log_content.find("INFO: ObjectStorageServer: started"), std::string::npos);
}
