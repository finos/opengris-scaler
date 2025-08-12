#include "scaler/object_storage/object_storage_server.h"

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

void ObjectStorageServer::run(std::string name, std::string port)
{
    try {
        tcp::resolver resolver(ioContext);
        auto res = resolver.resolve(name, port);

        boost::asio::signal_set signals(ioContext, SIGINT, SIGTERM);
        signals.async_wait([&](auto, auto) { ioContext.stop(); });

        co_spawn(ioContext, listener(res.begin()->endpoint()), detached);
        ioContext.run();
    } catch (std::exception& e) {
        log(scaler::ymq::LoggingLevel::error, "Exception: ", e.what(), "\n",
        "Mostly something serious happen, inspect capnp header corruption \n");
        std::terminate();
    }
}

void ObjectStorageServer::waitUntilReady()
{
    uint64_t value;
    ssize_t ret = read(onServerReadyReader, &value, sizeof(uint64_t));

    if (ret != sizeof(uint64_t)) {
        log(scaler::ymq::LoggingLevel::error, "read from onServerReadyReader failed: errno=", errno , "\n");
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
        log(scaler::ymq::LoggingLevel::error, "create on server ready FDs failed: errno=", errno, "\n");
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
        log(scaler::ymq::LoggingLevel::error, "write to onServerReadyWriter failed: errno=", errno, "\n");
        std::terminate();
    }
}

void ObjectStorageServer::closeServerReadyFds()
{
    const std::array<int, 2> fds {onServerReadyReader, onServerReadyWriter};

    for (const int fd: fds) {
        if (close(fd) != 0) {
            log(scaler::ymq::LoggingLevel::error, "close failed: errno=", errno, "\n");
            std::terminate();
        }
    }
}

awaitable<void> ObjectStorageServer::listener(tcp::endpoint endpoint)
{
    auto executor = co_await boost::asio::this_coro::executor;
    tcp::acceptor acceptor(executor, endpoint);

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
    } catch (std::exception& e) {
        log(scaler::ymq::LoggingLevel::error, "process_requests Exception: ", e.what(), "\n");
    }
}

awaitable<void> ObjectStorageServer::processSetRequest(
    std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader)
{
    if (requestHeader.payloadLength > MEMORY_LIMIT_IN_BYTES) {
        log(scaler::ymq::LoggingLevel::error, "payload length is larger than MEMORY_LIMIT_IN_BYTES = ", MEMORY_LIMIT_IN_BYTES, "\n");
        std::terminate();
    }

    if (requestHeader.payloadLength > SIZE_MAX) {
        log(scaler::ymq::LoggingLevel::error, "payload length is larger than SIZE_MAX = ", SIZE_MAX, "\n");
        std::terminate();
    }

    ObjectPayload requestPayload;
    requestPayload.resize(requestHeader.payloadLength);

    try {
        co_await boost::asio::async_read(client->socket, boost::asio::buffer(requestPayload), use_awaitable);
    } catch (boost::system::system_error& e) {
        log(scaler::ymq::LoggingLevel::error, "payload ends prematurely, e.what() = ", e.what(), "\n",
        "Failing fast. Terminting now...\n");
        std::terminate();
    }

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
        log(scaler::ymq::LoggingLevel::error, "payload length should be the size of ObjectID = ", ObjectID::bufferSize(), "\n");
        std::terminate();
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
