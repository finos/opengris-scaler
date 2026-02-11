#include <gtest/gtest.h>

#include <chrono>
#include <expected>
#include <future>
#include <string>

#include "scaler/uv_ymq/binder_socket.h"
#include "scaler/uv_ymq/io_context.h"
#include "scaler/uv_ymq/message_connection.h"
#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/bytes.h"

namespace {

const std::string messagePayload = "Hello YMQ!";

}  // namespace

// Helper class to set up a binder and client message connection pair
class BinderClientPair {
public:
    static const scaler::uv_ymq::Identity binderIdentity;
    static const scaler::uv_ymq::Identity clientIdentity;

    BinderClientPair(
        scaler::uv_ymq::IOContext& context,
        scaler::wrapper::uv::Loop& loop,
        scaler::uv_ymq::MessageConnection::RecvMessageCallback clientOnMessage,
        scaler::uv_ymq::MessageConnection::RemoteDisconnectCallback clientOnDisconnect)
        : _binder(context, binderIdentity)
        , _client(
              loop,
              clientIdentity,
              std::nullopt,
              [](scaler::uv_ymq::Identity identity) { ASSERT_EQ(identity, binderIdentity); },  // onRemoteIdentity
              std::move(clientOnDisconnect),
              std::move(clientOnMessage))
        , _clientSocket(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(loop)))
        , _loop(loop)
    {
        // Bind to an available port
        std::promise<scaler::uv_ymq::Address> bindPromise;
        std::future<scaler::uv_ymq::Address> bindFuture = bindPromise.get_future();

        _binder.bindTo(
            "tcp://127.0.0.1:0",
            [promise =
                 std::move(bindPromise)](std::expected<scaler::uv_ymq::Address, scaler::ymq::Error> result) mutable {
                ASSERT_TRUE(result.has_value());
                promise.set_value(result.value());
            });

        scaler::uv_ymq::Address boundAddress = bindFuture.get();

        // Connect the client to the binder
        _clientSocket.connect(boundAddress.asTCP(), [this](std::expected<void, scaler::wrapper::uv::Error>) {
            _client.connect(std::move(_clientSocket));
        });
    }

    scaler::uv_ymq::BinderSocket& binder() { return _binder; }
    scaler::uv_ymq::MessageConnection& client() { return _client; }
    scaler::wrapper::uv::Loop& loop() { return _loop; }

private:
    scaler::uv_ymq::BinderSocket _binder;
    scaler::uv_ymq::MessageConnection _client;
    scaler::wrapper::uv::TCPSocket _clientSocket;
    scaler::wrapper::uv::Loop& _loop;
};

const scaler::uv_ymq::Identity BinderClientPair::binderIdentity = "binder-identity";
const scaler::uv_ymq::Identity BinderClientPair::clientIdentity = "client-identity";

class UVYMQBinderSocketTest: public ::testing::Test {};

TEST_F(UVYMQBinderSocketTest, BindTo)
{
    // Test that a BinderSocket can successfully bind to a TCP address

    scaler::uv_ymq::IOContext context {};
    scaler::uv_ymq::BinderSocket binder {context, BinderClientPair::binderIdentity};

    ASSERT_EQ(binder.identity(), BinderClientPair::binderIdentity);

    std::promise<void> bindCalled {};

    binder.bindTo("tcp://127.0.0.1:0", [&](std::expected<scaler::uv_ymq::Address, scaler::ymq::Error> result) mutable {
        ASSERT_TRUE(result.has_value());
        bindCalled.set_value();
    });

    // Wait for bind to complete
    ASSERT_EQ(bindCalled.get_future().wait_for(std::chrono::seconds {1}), std::future_status::ready);
}

TEST_F(UVYMQBinderSocketTest, SendMessage)
{
    // Test that messages can be sent before and after a connection is established

    scaler::uv_ymq::IOContext context {};
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    bool clientMessageReceived = false;

    auto onClientRecvMessage = [&](scaler::ymq::Bytes receivedPayload) {
        ASSERT_EQ(receivedPayload.as_string(), messagePayload);
        clientMessageReceived = true;
    };

    auto onClientDisconnect = [](auto) { FAIL() << "Unexpected disconnect on client"; };

    BinderClientPair connections(context, loop, std::move(onClientRecvMessage), std::move(onClientDisconnect));

    // Send a message to the client's identity BEFORE the client connects

    std::promise<void> sendCallbackCalled {};

    auto onBinderMessageSent = [&](std::expected<void, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        sendCallbackCalled.set_value();
    };

    connections.binder().sendMessage(
        BinderClientPair::clientIdentity, scaler::ymq::Bytes(messagePayload), onBinderMessageSent);

    // Wait for the client to receive the first message (sent before connection)

    while (!clientMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(sendCallbackCalled.get_future().wait_for(std::chrono::seconds {5}), std::future_status::ready);

    // Send a message AFTER the client connected

    clientMessageReceived = false;
    sendCallbackCalled    = {};

    connections.binder().sendMessage(
        BinderClientPair::clientIdentity, scaler::ymq::Bytes(messagePayload), onBinderMessageSent);

    // Wait for the client to receive the second message

    while (!clientMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(sendCallbackCalled.get_future().wait_for(std::chrono::seconds {5}), std::future_status::ready);
}

TEST_F(UVYMQBinderSocketTest, RecvMessage)
{
    // Test that the binder can receive messages

    scaler::uv_ymq::IOContext context {};
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    auto onClientRecvMessage = [](scaler::ymq::Bytes) { FAIL() << "Unexpected message on client"; };

    auto onClientDisconnect = [](auto) { FAIL() << "Unexpected disconnect on client"; };

    BinderClientPair connections(context, loop, std::move(onClientRecvMessage), std::move(onClientDisconnect));

    // Register a first receive callback BEFORE the client connects

    std::promise<scaler::ymq::Message> recvCalled {};

    auto onBinderRecvMessage = [&](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        recvCalled.set_value(result.value());
    };

    connections.binder().recvMessage(onBinderRecvMessage);

    // Make the client send the first message

    bool sendCalled    = false;
    auto onMessageSent = [&](std::expected<void, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        sendCalled = true;
    };

    connections.client().sendMessage(scaler::ymq::Bytes(messagePayload), onMessageSent);

    while (!sendCalled) {
        loop.run(UV_RUN_NOWAIT);
    }

    // Validate the message on the binder

    scaler::ymq::Message message = recvCalled.get_future().get();
    ASSERT_EQ(message.address.as_string(), BinderClientPair::clientIdentity);
    ASSERT_EQ(message.payload.as_string(), messagePayload);

    // Register a 2nd receive callback, AFTER the client connected

    recvCalled = {};
    connections.binder().recvMessage(onBinderRecvMessage);

    // Make the client send the second message

    sendCalled = false;
    connections.client().sendMessage(scaler::ymq::Bytes(messagePayload), onMessageSent);

    while (!sendCalled) {
        loop.run(UV_RUN_NOWAIT);
    }

    // Validate the binder receives the 2nd message

    message = recvCalled.get_future().get();
    ASSERT_EQ(message.address.as_string(), BinderClientPair::clientIdentity);
    ASSERT_EQ(message.payload.as_string(), messagePayload);
}

TEST_F(UVYMQBinderSocketTest, CloseConnection)
{
    // Test that the client receives a disconnect event when the binder calls closeConnection()

    scaler::uv_ymq::IOContext context {};
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());

    bool clientDisconnected = false;

    auto onClientRecvMessage = [](scaler::ymq::Bytes) { FAIL() << "Unexpected message on client"; };

    auto onClientDisconnect = [&](scaler::uv_ymq::MessageConnection::DisconnectReason reason) {
        ASSERT_EQ(reason, scaler::uv_ymq::MessageConnection::DisconnectReason::Disconnected);
        clientDisconnected = true;
    };

    BinderClientPair connections(context, loop, std::move(onClientRecvMessage), std::move(onClientDisconnect));

    // Wait for connection to be established
    while (!connections.client().established()) {
        loop.run(UV_RUN_ONCE);
    }

    // Call closeConnection() on the binder
    connections.binder().closeConnection(BinderClientPair::clientIdentity);

    // Validate that the client receives a disconnect event
    while (!clientDisconnected) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_FALSE(connections.client().connected());
}
