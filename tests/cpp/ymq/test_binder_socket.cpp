#include <gtest/gtest.h>

#include <chrono>
#include <expected>
#include <future>
#include <string>

#include "scaler/wrapper/uv/callback.h"
#include "scaler/wrapper/uv/error.h"
#include "scaler/wrapper/uv/loop.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/binder_socket.h"
#include "scaler/ymq/buffered_bytes.h"
#include "scaler/ymq/bytes.h"
#include "scaler/ymq/internal/message_connection.h"
#include "scaler/ymq/io_context.h"

namespace {

const std::string messagePayload = "Hello YMQ!";

}  // namespace

// Helper class to set up a binder and client message connection pair
class BinderClientPair {
public:
    static const scaler::ymq::Identity binderIdentity;
    static const scaler::ymq::Identity clientIdentity;

    BinderClientPair(
        scaler::ymq::internal::MessageConnection::RecvMessageCallback clientOnMessage,
        scaler::ymq::internal::MessageConnection::RemoteDisconnectCallback clientOnDisconnect)
        : _context()
        , _loop(UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init()))
        , _binder(_context, binderIdentity)
        , _client(
              clientIdentity,
              std::nullopt,
              [](scaler::ymq::Identity identity) { ASSERT_EQ(identity, binderIdentity); },  // onRemoteIdentity
              std::move(clientOnDisconnect),
              std::move(clientOnMessage))
        , _clientSocket(UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(_loop)))
    {
        // Bind to an available port
        std::promise<scaler::ymq::Address> bindPromise;
        std::future<scaler::ymq::Address> bindFuture = bindPromise.get_future();

        _binder.bindTo(
            "tcp://127.0.0.1:0",
            [promise = std::move(bindPromise)](std::expected<scaler::ymq::Address, scaler::ymq::Error> result) mutable {
                ASSERT_TRUE(result.has_value());
                promise.set_value(result.value());
            });

        scaler::ymq::Address boundAddress = bindFuture.get();

        // Connect the client to the binder
        _clientSocket.connect(boundAddress.asTCP(), [this](std::expected<void, scaler::wrapper::uv::Error>) {
            _client.connect(scaler::ymq::internal::Client(std::move(_clientSocket)));
        });
    }

    scaler::ymq::BinderSocket& binder()
    {
        return _binder;
    }
    scaler::ymq::internal::MessageConnection& client()
    {
        return _client;
    }
    scaler::wrapper::uv::Loop& loop()
    {
        return _loop;
    }

private:
    scaler::ymq::IOContext _context;
    scaler::wrapper::uv::Loop _loop;
    scaler::ymq::BinderSocket _binder;
    scaler::ymq::internal::MessageConnection _client;
    scaler::wrapper::uv::TCPSocket _clientSocket;
};

const scaler::ymq::Identity BinderClientPair::binderIdentity = "binder-identity";
const scaler::ymq::Identity BinderClientPair::clientIdentity = "client-identity";

class YMQBinderSocketTest: public ::testing::Test {};

TEST_F(YMQBinderSocketTest, BindTo)
{
    // Test that a BinderSocket can successfully bind to a TCP address

    scaler::ymq::IOContext context {};
    scaler::ymq::BinderSocket binder {context, BinderClientPair::binderIdentity};

    ASSERT_EQ(binder.identity(), BinderClientPair::binderIdentity);

    std::promise<void> bindCalled {};

    binder.bindTo("tcp://127.0.0.1:0", [&](std::expected<scaler::ymq::Address, scaler::ymq::Error> result) mutable {
        ASSERT_TRUE(result.has_value());
        bindCalled.set_value();
    });

    // Wait for bind to complete
    ASSERT_EQ(bindCalled.get_future().wait_for(std::chrono::seconds {1}), std::future_status::ready);
}

TEST_F(YMQBinderSocketTest, SendMessage)
{
    // Test that messages can be sent before and after a connection is established

    bool clientMessageReceived = false;

    auto onClientRecvMessage = [&](std::unique_ptr<scaler::ymq::Bytes> receivedPayload) {
        ASSERT_EQ(scaler::ymq::asString(*receivedPayload), messagePayload);
        clientMessageReceived = true;
    };

    auto onClientDisconnect = [](auto) { FAIL() << "Unexpected disconnect on client"; };

    BinderClientPair connections(std::move(onClientRecvMessage), std::move(onClientDisconnect));

    scaler::ymq::BinderSocket& binder = connections.binder();
    scaler::wrapper::uv::Loop& loop   = connections.loop();

    // Send a message to the client's identity BEFORE the client connects

    std::promise<void> sendCallbackCalled {};

    auto onBinderMessageSent = [&](std::expected<void, scaler::ymq::Error> result,
                                   [[maybe_unused]] std::unique_ptr<scaler::ymq::Bytes>) {
        ASSERT_TRUE(result.has_value());
        sendCallbackCalled.set_value();
    };

    binder.sendMessage(
        BinderClientPair::clientIdentity,
        std::make_unique<scaler::ymq::BufferedBytes>(messagePayload),
        onBinderMessageSent);

    // Wait for the client to receive the first message (sent before connection)

    while (!clientMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(sendCallbackCalled.get_future().wait_for(std::chrono::seconds {5}), std::future_status::ready);

    // Send a message AFTER the client connected

    clientMessageReceived = false;
    sendCallbackCalled    = {};

    binder.sendMessage(
        BinderClientPair::clientIdentity,
        std::make_unique<scaler::ymq::BufferedBytes>(messagePayload),
        onBinderMessageSent);

    // Wait for the client to receive the second message

    while (!clientMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(sendCallbackCalled.get_future().wait_for(std::chrono::seconds {5}), std::future_status::ready);
}

TEST_F(YMQBinderSocketTest, SendMulticastMessage)
{
    // Test that the binder can multicast and broadcast messages

    bool clientMessageReceived = false;

    auto onClientRecvMessage = [&](std::unique_ptr<scaler::ymq::Bytes> receivedPayload) {
        ASSERT_FALSE(clientMessageReceived);
        ASSERT_EQ(scaler::ymq::asString(*receivedPayload), messagePayload);
        clientMessageReceived = true;
    };

    auto onClientDisconnect = [](auto) { FAIL() << "Unexpected disconnect on client"; };

    BinderClientPair connections(std::move(onClientRecvMessage), std::move(onClientDisconnect));

    scaler::ymq::BinderSocket& binder = connections.binder();
    scaler::wrapper::uv::Loop& loop   = connections.loop();

    // Make sure the client is ready

    binder.sendMessage(
        BinderClientPair::clientIdentity, std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), [](auto, auto) {
        });

    while (!clientMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }

    clientMessageReceived = false;

    // Send a broadcast message

    binder.sendMulticastMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload));

    while (!clientMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }

    clientMessageReceived = false;

    // Send two multicast messages, should only receive the one with the matching prefix

    binder.sendMulticastMessage(
        std::make_unique<scaler::ymq::BufferedBytes>("unexpected multicast message"), "invalid-prefix");
    binder.sendMulticastMessage(
        std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), BinderClientPair::clientIdentity.substr(0, 5));

    while (!clientMessageReceived) {
        loop.run(UV_RUN_ONCE);
    }
}

TEST_F(YMQBinderSocketTest, RecvMessage)
{
    // Test that the binder can receive messages

    auto onClientRecvMessage = [](std::unique_ptr<scaler::ymq::Bytes>) { FAIL() << "Unexpected message on client"; };

    auto onClientDisconnect = [](auto) { FAIL() << "Unexpected disconnect on client"; };

    BinderClientPair connections(std::move(onClientRecvMessage), std::move(onClientDisconnect));

    scaler::ymq::BinderSocket& binder                = connections.binder();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Register a first receive callback BEFORE the client connects

    std::promise<scaler::ymq::Message> recvCalled {};

    auto onBinderRecvMessage = [&](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        recvCalled.set_value(std::move(*result));
    };

    binder.recvMessage(onBinderRecvMessage);

    // Make the client send the first message

    bool sendCalled    = false;
    auto onMessageSent = [&](std::expected<void, scaler::ymq::Error> result,
                             [[maybe_unused]] std::unique_ptr<scaler::ymq::Bytes>) {
        ASSERT_TRUE(result.has_value());
        sendCalled = true;
    };

    client.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), onMessageSent);

    while (!sendCalled) {
        loop.run(UV_RUN_NOWAIT);
    }

    // Validate the message on the binder

    scaler::ymq::Message message = recvCalled.get_future().get();
    ASSERT_EQ(scaler::ymq::asString(*message.address), BinderClientPair::clientIdentity);
    ASSERT_EQ(scaler::ymq::asString(*message.payload), messagePayload);

    // Register a 2nd receive callback, AFTER the client connected

    recvCalled = {};
    binder.recvMessage(onBinderRecvMessage);

    // Make the client send the second message

    sendCalled = false;
    client.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), onMessageSent);

    while (!sendCalled) {
        loop.run(UV_RUN_NOWAIT);
    }

    // Validate the binder receives the 2nd message

    message = recvCalled.get_future().get();
    ASSERT_EQ(scaler::ymq::asString(*message.address), BinderClientPair::clientIdentity);
    ASSERT_EQ(scaler::ymq::asString(*message.payload), messagePayload);
}

TEST_F(YMQBinderSocketTest, CloseConnection)
{
    // Test that the client receives a disconnect event when the binder calls closeConnection()

    bool clientDisconnected = false;

    auto onClientRecvMessage = [](std::unique_ptr<scaler::ymq::Bytes>) { FAIL() << "Unexpected message on client"; };

    auto onClientDisconnect = [&](scaler::ymq::internal::MessageConnection::DisconnectReason reason) {
        ASSERT_EQ(reason, scaler::ymq::internal::MessageConnection::DisconnectReason::Disconnected);
        clientDisconnected = true;
    };

    BinderClientPair connections(std::move(onClientRecvMessage), std::move(onClientDisconnect));

    scaler::ymq::BinderSocket& binder                = connections.binder();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Make a single message exchange to ensure the connection is established

    std::promise<scaler::ymq::Message> binderRecvCalled;
    auto onBinderRecvMessage = [&](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
        binderRecvCalled.set_value(std::move(*result));
    };
    binder.recvMessage(onBinderRecvMessage);

    // Send a message from the client to the binder
    bool sendCalled    = false;
    auto onMessageSent = [&]([[maybe_unused]] std::expected<void, scaler::ymq::Error> result,
                             [[maybe_unused]] std::unique_ptr<scaler::ymq::Bytes>) { sendCalled = true; };
    client.sendMessage(std::make_unique<scaler::ymq::BufferedBytes>(messagePayload), onMessageSent);

    while (!sendCalled) {
        loop.run(UV_RUN_NOWAIT);
    }

    // Wait for the binder to receive the message
    binderRecvCalled.get_future().wait_for(std::chrono::seconds {1});

    // Call closeConnection() on the binder

    binder.closeConnection(BinderClientPair::clientIdentity);

    // Validate that the client receives a disconnect event
    while (!clientDisconnected) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_FALSE(client.connected());
}

TEST_F(YMQBinderSocketTest, SendToClosedIdentityFailsFast)
{
    // Regression test: after the binder has terminated a peer connection (either via an
    // explicit closeConnection() or by observing the peer's graceful FIN), a subsequent
    // sendMessage to that identity must resolve its callback with
    // ConnectorSocketClosedByRemoteEnd rather than queue the callback in
    // _pendingSendMessages forever. The latter was the scheduler hang reproduced by
    // examples/graphtask_nested_client.py.
    //
    // We use closeConnection() here because it is the deterministic path that exercises the
    // same _disconnectedIdentities/drain branch on every platform. (Driving the test via the
    // client's graceful disconnect would, on macOS in particular, sometimes get reported to
    // the binder as Aborted rather than Disconnected -- which is correctly NOT terminal per
    // YMQ's Aborted-means-reconnect-expected semantic, so the assertion would fail for a
    // reason unrelated to the bug under test.)

    bool clientDisconnected  = false;
    auto onClientRecvMessage = [](std::unique_ptr<scaler::ymq::Bytes>) { FAIL() << "Unexpected message on client"; };
    auto onClientDisconnect  = [&](scaler::ymq::internal::MessageConnection::DisconnectReason reason) {
        ASSERT_EQ(reason, scaler::ymq::internal::MessageConnection::DisconnectReason::Disconnected);
        clientDisconnected = true;
    };

    BinderClientPair connections(std::move(onClientRecvMessage), std::move(onClientDisconnect));

    scaler::ymq::BinderSocket& binder                = connections.binder();
    scaler::ymq::internal::MessageConnection& client = connections.client();
    scaler::wrapper::uv::Loop& loop                  = connections.loop();

    // Establish the connection by exchanging a message (forces identity exchange).

    std::promise<scaler::ymq::Message> binderRecvCalled;
    binder.recvMessage([&](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
        binderRecvCalled.set_value(std::move(result.value()));
    });

    bool clientSendCalled = false;
    client.sendMessage(
        std::make_unique<scaler::ymq::BufferedBytes>(messagePayload),
        [&]([[maybe_unused]] std::expected<void, scaler::ymq::Error> result,
            [[maybe_unused]] std::unique_ptr<scaler::ymq::Bytes>) { clientSendCalled = true; });

    while (!clientSendCalled) {
        loop.run(UV_RUN_NOWAIT);
    }
    ASSERT_EQ(binderRecvCalled.get_future().wait_for(std::chrono::seconds {1}), std::future_status::ready);

    // Explicitly close the connection on the binder side, and drive the loop until the
    // client has observed the resulting disconnect. After this point the binder thread has
    // definitely processed closeConnection (the client wouldn't have been notified otherwise).

    binder.closeConnection(BinderClientPair::clientIdentity);
    while (!clientDisconnected) {
        loop.run(UV_RUN_ONCE);
    }

    // Pre-fix: this send lands in _pendingSendMessages[clientIdentity] and stays there
    // forever -- the callback never fires and the test hits the 5s wait_for timeout.
    // Post-fix: closeConnection has populated _disconnectedIdentities, so the callback fires
    // immediately with ConnectorSocketClosedByRemoteEnd.
    //
    // The result holder is kept alive via shared_ptr so a late callback after this test
    // function returns can't deref a destroyed promise.

    auto sendResult = std::make_shared<std::promise<std::expected<void, scaler::ymq::Error>>>();
    binder.sendMessage(
        BinderClientPair::clientIdentity,
        std::make_unique<scaler::ymq::BufferedBytes>(messagePayload),
        [sendResult](
            std::expected<void, scaler::ymq::Error> result,
            [[maybe_unused]] std::unique_ptr<scaler::ymq::Bytes>) noexcept {
            try {
                sendResult->set_value(std::move(result));
            } catch (...) {
            }
        });

    auto sendFuture = sendResult->get_future();
    ASSERT_EQ(sendFuture.wait_for(std::chrono::seconds {5}), std::future_status::ready)
        << "send callback did not fire: regression of the binder hang for closed peers";

    auto result = sendFuture.get();
    ASSERT_FALSE(result.has_value());
    ASSERT_EQ(result.error()._errorCode, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
}

TEST_F(YMQBinderSocketTest, StopRequested)
{
    scaler::ymq::IOContext context {};
    scaler::wrapper::uv::Loop loop = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Loop::init());
    std::optional<scaler::ymq::BinderSocket> binder =
        scaler::ymq::BinderSocket {context, BinderClientPair::binderIdentity};

    binder->bindTo("tcp://127.0.0.1:0", [](std::expected<scaler::ymq::Address, scaler::ymq::Error> result) {
        ASSERT_TRUE(result.has_value());
    });

    // Queue a receive call

    std::optional<scaler::ymq::Error> recvError {};

    binder->recvMessage([&](std::expected<scaler::ymq::Message, scaler::ymq::Error> result) {
        ASSERT_FALSE(result.has_value());
        recvError = result.error();
    });

    // Queue a send call to a not yet connected client

    std::optional<scaler::ymq::Error> sendError {};

    binder->sendMessage(
        "unknown-client",
        std::make_unique<scaler::ymq::BufferedBytes>(messagePayload),
        [&](std::expected<void, scaler::ymq::Error> result, [[maybe_unused]] std::unique_ptr<scaler::ymq::Bytes>) {
            ASSERT_FALSE(result.has_value());
            sendError = result.error();
        });

    // Destroy the binder, expect the receive/send results to be filled with the SocketStopRequested error

    binder = std::nullopt;

    while (!recvError.has_value() || !sendError.has_value()) {
        loop.run(UV_RUN_ONCE);
    }

    ASSERT_EQ(recvError->_errorCode, scaler::ymq::Error::ErrorCode::SocketStopRequested);
    ASSERT_EQ(sendError->_errorCode, scaler::ymq::Error::ErrorCode::SocketStopRequested);
}
