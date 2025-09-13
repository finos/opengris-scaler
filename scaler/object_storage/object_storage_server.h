#pragma once

#include <boost/asio.hpp>
#include <expected>
#include <iostream>
#include <memory>
#include <span>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/logging.h"
#include "scaler/io/ymq/simple_interface.h"
#include "scaler/object_storage/constants.h"
#include "scaler/object_storage/defs.h"
#include "scaler/object_storage/io_helper.h"
#include "scaler/object_storage/message.h"
#include "scaler/object_storage/object_manager.h"

namespace scaler {
namespace object_storage {

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;

class ObjectStorageServer {
public:
    using Identity          = ymq::Configuration::IOSocketIdentity;
    using SendMessageFuture = std::future<std::expected<void, ymq::Error>>;
    using GetResponseResult = std::pair<SendMessageFuture, SendMessageFuture>;
    ObjectStorageServer();

    ~ObjectStorageServer();

    void run(
        std::string name,
        std::string port,
        ymq::Configuration::IOSocketIdentity identity = "ObjectStorageServer",
        std::string log_level                         = "INFO",
        std::string log_format                        = "%(levelname)s: %(message)s",
        std::vector<std::string> log_paths            = {"/dev/stdout"});

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

    std::atomic<bool> _stopped;

    void initServerReadyFds();

    void setServerReadyFd();

    void closeServerReadyFds();

    ObjectRequestHeader parseRequestHeader(Bytes message);

    void processRequests();

    std::vector<SendMessageFuture> processSetRequest(
        std::shared_ptr<Client> client, std::pair<ObjectRequestHeader, Bytes> request);

    GetResponseResult processGetRequest(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader);

    GetResponseResult processDeleteRequest(std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader);

    std::vector<SendMessageFuture> processDuplicateRequest(
        std::shared_ptr<Client> client, std::pair<ObjectRequestHeader, Bytes> request);

    template <ObjectStorageMessage T>
    auto writeMessage(std::shared_ptr<Client> client, T& message, std::span<const unsigned char> payload)
    {
        // Send OSS header
        auto messageBuffer = message.toBuffer();
        ymq::Message ymqHeader {};
        ymqHeader.address     = Bytes(client->_identity);
        ymqHeader.payload     = Bytes((char*)messageBuffer.asBytes().begin(), messageBuffer.asBytes().size());
        auto sendHeaderFuture = ymq::futureSendMessage(client->_ioSocket, std::move(ymqHeader));

        if (!payload.data()) {
            return std::pair {std::move(sendHeaderFuture), std::future<std::expected<void, ymq::Error>> {}};
        }

        ymq::Message ymqPayload {};
        ymqPayload.address     = Bytes(client->_identity);
        ymqPayload.payload     = Bytes((char*)payload.data(), payload.size());
        auto sendPayloadFuture = ymq::futureSendMessage(client->_ioSocket, std::move(ymqPayload));

        return std::pair {std::move(sendHeaderFuture), std::move(sendPayloadFuture)};
    }

    void catFuts(std::vector<ObjectStorageServer::SendMessageFuture>& dest, PairOrVecOf<SendMessageFuture> auto&& src)
    {
        if constexpr (requires { src.first; }) {
            dest.emplace_back(std::move(src.first));
            dest.emplace_back(std::move(src.second));
            return;
        }

        if constexpr (requires { src.begin(); }) {
            // NOTE: Not the std::move you might expect!
            dest.resize(dest.size() + src.size());
            auto first = dest.end() - src.size();
            std::move(src.begin(), src.end(), first);
            return;
        }
        assert(false);
    }

    GetResponseResult sendGetResponse(
        std::shared_ptr<Client> client,
        const ObjectRequestHeader& requestHeader,
        std::shared_ptr<const ObjectPayload> objectPtr);

    GetResponseResult sendDuplicateResponse(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader);

    std::vector<SendMessageFuture> optionallySendPendingRequests(
        const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr);
};

};  // namespace object_storage
};  // namespace scaler
