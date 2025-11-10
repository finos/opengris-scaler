#pragma once

#include <expected>
#include <iostream>
#include <memory>
#include <optional>
#include <span>

#include "scaler/logging/logging.h"
#include "scaler/object_storage/constants.h"
#include "scaler/object_storage/defs.h"
#include "scaler/object_storage/io_helper.h"
#include "scaler/object_storage/message.h"
#include "scaler/object_storage/object_manager.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/io_context.h"
#include "scaler/ymq/io_socket.h"
#include "scaler/ymq/simple_interface.h"

namespace scaler {
namespace object_storage {

class ObjectStorageServer {
public:
    using Identity          = ymq::Configuration::IOSocketIdentity;
    using SendMessageFuture = std::future<std::optional<ymq::Error>>;

    ObjectStorageServer();

    ~ObjectStorageServer();

    void run(
        std::string name,
        std::string port,
        Identity identity                  = "ObjectStorageServer",
        std::string log_level              = "INFO",
        std::string log_format             = "%(levelname)s: %(message)s",
        std::vector<std::string> log_paths = {"/dev/stdout"},
        std::function<bool()> running      = []() { return true; });

    void waitUntilReady();

    void shutdown();

private:
    struct Client {
        std::shared_ptr<ymq::IOSocket> _ioSocket;
        Identity _identity;
    };

    struct PendingRequest {
        std::shared_ptr<Client> client;
        ObjectRequestHeader requestHeader;
    };

    using ObjectRequestType  = scaler::protocol::ObjectRequestHeader::ObjectRequestType;
    using ObjectResponseType = scaler::protocol::ObjectResponseHeader::ObjectResponseType;

    ymq::IOContext _ioContext;
    std::shared_ptr<ymq::IOSocket> _ioSocket;

    int onServerReadyReader;
    int onServerReadyWriter;

    ObjectManager objectManager;

    // Some GET and DUPLICATE requests might be delayed if the referenced object isn't available yet.
    std::map<ObjectID, std::vector<PendingRequest>> pendingRequests;

    scaler::ymq::Logger _logger;

    std::vector<SendMessageFuture> _pendingSendMessageFuts;

    void initServerReadyFds();

    void setServerReadyFd();

    void closeServerReadyFds();

    void processRequests(std::function<bool()> stopCondition);

    void processSetRequest(std::shared_ptr<Client> client, std::pair<ObjectRequestHeader, Bytes> request);

    void processGetRequest(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader);

    void processDeleteRequest(std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader);

    void processDuplicateRequest(std::shared_ptr<Client> client, std::pair<ObjectRequestHeader, Bytes> request);

    void processInfoGetTotalRequest(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader);

    template <ObjectStorageMessage T>
    void writeMessage(std::shared_ptr<Client> client, T& message, std::span<const unsigned char> payload)
    {
        // Send OSS header
        auto messageBuffer = message.toBuffer();
        ymq::Message ymqHeader {};
        ymqHeader.address     = Bytes(client->_identity);
        ymqHeader.payload     = Bytes((char*)messageBuffer.asBytes().begin(), messageBuffer.asBytes().size());
        auto sendHeaderFuture = ymq::futureSendMessage(client->_ioSocket, std::move(ymqHeader));

        if (!payload.data()) {
            _pendingSendMessageFuts.emplace_back(std::move(sendHeaderFuture));
            return;
        }

        ymq::Message ymqPayload {};
        ymqPayload.address     = Bytes(client->_identity);
        ymqPayload.payload     = Bytes((char*)payload.data(), payload.size());
        auto sendPayloadFuture = ymq::futureSendMessage(client->_ioSocket, std::move(ymqPayload));

        _pendingSendMessageFuts.emplace_back(std::move(sendHeaderFuture));
        _pendingSendMessageFuts.emplace_back(std::move(sendPayloadFuture));
    }

    void sendGetResponse(
        std::shared_ptr<Client> client,
        const ObjectRequestHeader& requestHeader,
        std::shared_ptr<const ObjectPayload> objectPtr);

    void sendDuplicateResponse(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader);

    void optionallySendPendingRequests(const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr);
};

};  // namespace object_storage
};  // namespace scaler
