#include "scaler/object_storage/object_storage_server.h"

#include <exception>

namespace scaler {
namespace object_storage {

ObjectStorageServer::ObjectStorageServer()
{
    initServerReadyFds();
}

ObjectStorageServer::~ObjectStorageServer()
{
    shutdown();
    closeServerReadyFds();
}

void ObjectStorageServer::run(
    std::string name, std::string port, std::string log_level, std::string log_format, std::string log_path)
{
    _logger = scaler::ymq::Logger(log_format, log_path, scaler::ymq::Logger::stringToLogLevel(log_level));

    try {
        tcp::resolver resolver(ioContext);
        auto res = resolver.resolve(name, port);

        boost::asio::signal_set signals(ioContext, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { ioContext.stop(); });

        co_spawn(ioContext, listener(res.begin()->endpoint()), detached);
        ioContext.run();
    } catch (const std::exception& e) {
        _logger.log(
            scaler::ymq::Logger::LoggingLevel::error,
            "ObjectStorageServer: unexpected server error, reason: ",
            e.what());
    }
}

void ObjectStorageServer::waitUntilReady()
{
    uint64_t value;
    ssize_t ret = read(onServerReadyReader, &value, sizeof(uint64_t));

    if (ret != sizeof(uint64_t)) {
        _logger.log(
            scaler::ymq::Logger::LoggingLevel::error,
            "ObjectStorageServer: read from onServerReadyReader failed, errno=",
            errno);
        std::terminate();
    }
}

void ObjectStorageServer::shutdown()
{
    ioContext.stop();
}

void ObjectStorageServer::initServerReadyFds()
{
    int pipeFds[2] {};
    int ret = pipe(pipeFds);

    if (ret != 0) {
        _logger.log(
            scaler::ymq::Logger::LoggingLevel::error,
            "ObjectStorageServer: create on server ready FDs failed, errno=",
            errno);
        std::terminate();
    }

    onServerReadyReader = pipeFds[0];
    onServerReadyWriter = pipeFds[1];
}

void ObjectStorageServer::setServerReadyFd()
{
    uint64_t value = 1;
    ssize_t ret    = write(onServerReadyWriter, &value, sizeof(uint64_t));

    if (ret != sizeof(uint64_t)) {
        _logger.log(
            scaler::ymq::Logger::LoggingLevel::error,
            "ObjectStorageServer: write to onServerReadyWriter failed, errno=",
            errno);
        std::terminate();
    }
}

void ObjectStorageServer::closeServerReadyFds()
{
    const std::array<int, 2> fds {onServerReadyReader, onServerReadyWriter};

    for (const int fd: fds) {
        if (close(fd) != 0) {
            _logger.log(scaler::ymq::Logger::LoggingLevel::error, "ObjectStorageServer: close failed, errno=", errno);
            std::terminate();
        }
    }
}

awaitable<void> ObjectStorageServer::listener(tcp::endpoint endpoint)
{
    auto executor = co_await boost::asio::this_coro::executor;
    tcp::acceptor acceptor(executor, endpoint);

    _logger.log(scaler::ymq::Logger::LoggingLevel::info, "ObjectStorageServer: started");

    setServerReadyFd();

    for (;;) {
        auto client = std::make_shared<Client>(executor);

        co_await acceptor.async_accept(client->socket, use_awaitable);
        setTCPNoDelay(client->socket, true);

        co_spawn(executor, processRequests(client), detached);
    }
}

awaitable<void> ObjectStorageServer::processRequests(std::shared_ptr<Client> client)
{
    _logger.log(scaler::ymq::Logger::LoggingLevel::info, "ObjectStorageServer: client connected");

    try {
        for (;;) {
            ObjectRequestHeader requestHeader = co_await readMessage<ObjectRequestHeader>(client);

            switch (requestHeader.requestType) {
                case ObjectRequestType::SET_OBJECT: {
                    co_await processSetRequest(client, requestHeader);
                    break;
                }
                case ObjectRequestType::GET_OBJECT: {
                    co_await processGetRequest(client, requestHeader);
                    break;
                }
                case ObjectRequestType::DELETE_OBJECT: {
                    co_await processDeleteRequest(client, requestHeader);
                    break;
                }
                case ObjectRequestType::DUPLICATE_OBJECT_I_D: {
                    co_await processDuplicateRequest(client, requestHeader);
                    break;
                }
            }
        }
    } catch (const boost::system::system_error& e) {
        if (e.code() == boost::asio::error::eof || e.code() == boost::asio::error::connection_reset) {
            _logger.log(scaler::ymq::Logger::LoggingLevel::info, "ObjectStorageServer: client disconnected");
        } else {
            _logger.log(
                scaler::ymq::Logger::LoggingLevel::error,
                "ObjectStorageServer: unexpected networking error, reason: ",
                e.what());
        }
    } catch (const std::exception& e) {
        _logger.log(
            scaler::ymq::Logger::LoggingLevel::error, "ObjectStorageServer: unexpected error, reason: ", e.what());
    }

    client->socket.close();
}

awaitable<void> ObjectStorageServer::processSetRequest(
    std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader)
{
    if (requestHeader.payloadLength > MEMORY_LIMIT_IN_BYTES) {
        throw std::runtime_error(
            "payload length is larger than MEMORY_LIMIT_IN_BYTES=" + std::to_string(MEMORY_LIMIT_IN_BYTES));
    }

    if (requestHeader.payloadLength > std::numeric_limits<size_t>::max()) {
        throw std::runtime_error("payload length is larger than SIZE_MAX=" + std::to_string(SIZE_MAX));
    }

    ObjectPayload requestPayload;
    requestPayload.resize(requestHeader.payloadLength);

    co_await boost::asio::async_read(client->socket, boost::asio::buffer(requestPayload), use_awaitable);

    auto objectPtr = objectManager.setObject(requestHeader.objectID, std::move(requestPayload));

    co_await optionallySendPendingRequests(requestHeader.objectID, objectPtr);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::SET_O_K,
    };

    co_await writeMessage(client, responseHeader, {});
}

awaitable<void> ObjectStorageServer::processGetRequest(
    std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader)
{
    auto objectPtr = objectManager.getObject(requestHeader.objectID);

    if (objectPtr != nullptr) {
        co_await sendGetResponse(client, requestHeader, objectPtr);
    } else {
        // We don't have the object yet. Send the response later after once we receive the SET request.
        pendingRequests[requestHeader.objectID].emplace_back(client, requestHeader);
    }
}

awaitable<void> ObjectStorageServer::processDeleteRequest(
    std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader)
{
    bool success = objectManager.deleteObject(requestHeader.objectID);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = success ? ObjectResponseType::DEL_O_K : ObjectResponseType::DEL_NOT_EXISTS,
    };

    co_await writeMessage(client, responseHeader, {});
}

awaitable<void> ObjectStorageServer::processDuplicateRequest(
    std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader)
{
    if (requestHeader.payloadLength != ObjectID::bufferSize()) {
        throw std::runtime_error(
            "payload length should be size_of(ObjectID)=" + std::to_string(ObjectID::bufferSize()));
    }

    ObjectID originalObjectID = co_await readMessage<ObjectID>(client);

    auto objectPtr = objectManager.duplicateObject(originalObjectID, requestHeader.objectID);

    if (objectPtr != nullptr) {
        co_await optionallySendPendingRequests(requestHeader.objectID, objectPtr);
        co_await sendDuplicateResponse(client, requestHeader);
    } else {
        // We don't have the referenced original object yet. Send the response later once we receive the SET request.
        pendingRequests[originalObjectID].emplace_back(client, requestHeader);
    }
}

awaitable<void> ObjectStorageServer::sendGetResponse(
    std::shared_ptr<Client> client,
    const ObjectRequestHeader& requestHeader,
    std::shared_ptr<const ObjectPayload> objectPtr)
{
    uint64_t payloadLength = std::min(static_cast<uint64_t>(objectPtr->size()), requestHeader.payloadLength);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = payloadLength,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::GET_O_K,
    };

    co_await writeMessage(client, responseHeader, {objectPtr->data(), payloadLength});
}

awaitable<void> ObjectStorageServer::sendDuplicateResponse(
    std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader)
{
    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::DUPLICATE_O_K,
    };

    co_await writeMessage(client, responseHeader, {});
}

awaitable<void> ObjectStorageServer::optionallySendPendingRequests(
    const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr)
{
    auto it = pendingRequests.find(objectID);

    if (it == pendingRequests.end()) {
        co_return;
    }

    // Immediately remove the object's pending requests, or else another coroutine might process them too.
    auto requests = std::move(it->second);
    pendingRequests.erase(it);

    for (auto& request: requests) {
        if (request.requestHeader.requestType == ObjectRequestType::GET_OBJECT) {
            if (request.client->socket.is_open()) {
                co_await sendGetResponse(request.client, request.requestHeader, objectPtr);
            }
        } else {
            assert(request.requestHeader.requestType == ObjectRequestType::DUPLICATE_OBJECT_I_D);

            objectManager.duplicateObject(objectID, request.requestHeader.objectID);

            if (request.client->socket.is_open()) {
                co_await sendDuplicateResponse(request.client, request.requestHeader);
            }

            // Some other pending requests might be themselves dependent on this duplicated object.
            co_await optionallySendPendingRequests(request.requestHeader.objectID, objectPtr);
        }
    }
}
};  // namespace object_storage
};  // namespace scaler
