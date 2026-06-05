#include <gtest/gtest.h>

#include <expected>
#include <string>
#include <thread>

#include "scaler/ymq/bytes.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/sync/binder_socket.h"
#include "scaler/ymq/sync/connector_socket.h"

namespace {

const std::string messagePayload = "Hello Sync YMQ!";

}  // namespace

class YMQSyncTest: public ::testing::Test {};

TEST_F(YMQSyncTest, BasicMessageExchange)
{
    // Test basic message exchange between a sync::BinderSocket and sync::ConnectorSocket

    const scaler::ymq::Identity binderIdentity    = "sync-binder";
    const scaler::ymq::Identity connectorIdentity = "sync-connector";

    scaler::ymq::IOContext context {};

    // Create and bind the binder socket
    scaler::ymq::sync::BinderSocket binder {context, binderIdentity};

    auto bindResult = binder.bindTo("tcp://127.0.0.1:0");
    ASSERT_TRUE(bindResult.has_value());

    scaler::ymq::Address boundAddress = bindResult.value();

    // Create connector socket in a separate thread to avoid blocking
    std::jthread connectorThread([&]() {
        auto connectorResult =
            scaler::ymq::sync::ConnectorSocket::connect(context, connectorIdentity, boundAddress.toString().value());

        ASSERT_TRUE(connectorResult.has_value());

        scaler::ymq::sync::ConnectorSocket connector = std::move(connectorResult.value());

        // Send message from connector to binder
        auto sendResult = connector.sendMessage(scaler::ymq::Bytes(messagePayload));
        ASSERT_TRUE(sendResult.has_value());

        // Receive response from binder
        auto recvResult = connector.recvMessage();
        ASSERT_TRUE(recvResult.has_value());

        ASSERT_EQ(recvResult.value().address.as_string(), binderIdentity);
        ASSERT_EQ(recvResult.value().payload.as_string(), messagePayload);

        // Binder should've closed the connection by now
        recvResult = connector.recvMessage();
        ASSERT_FALSE(recvResult.has_value());
        ASSERT_EQ(recvResult.error()._errorCode, scaler::ymq::Error::ErrorCode::ConnectorSocketClosedByRemoteEnd);
    });

    // Receive message on binder
    auto recvResult = binder.recvMessage();
    ASSERT_TRUE(recvResult.has_value());

    scaler::ymq::Message message = recvResult.value();
    ASSERT_EQ(message.address.as_string(), connectorIdentity);
    ASSERT_EQ(message.payload.as_string(), messagePayload);

    // Send response back to connector
    auto sendResult = binder.sendMessage(connectorIdentity, scaler::ymq::Bytes(messagePayload));
    ASSERT_TRUE(sendResult.has_value());

    // Request connector to disconnect
    binder.closeConnection(connectorIdentity);

    // Ensures the connector thread finishes
    connectorThread.join();
}
