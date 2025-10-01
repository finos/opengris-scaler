#include "scaler/object_storage/object_storage_server.h"

#include <algorithm>
#include <exception>
#include <future>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/simple_interface.h"
#include "scaler/io/ymq/typedefs.h"
#include "scaler/object_storage/message.h"

namespace scaler {
namespace object_storage {

ObjectStorageServer::ObjectStorageServer()
{
    initServerReadyFds();
}

ObjectStorageServer::~ObjectStorageServer()
{
    closeServerReadyFds();
}

void ObjectStorageServer::run(
    std::string name,
    std::string port,
    ObjectStorageServer::Identity identity,
    std::string log_level,
    std::string log_format,
    std::vector<std::string> log_paths)
{
    _logger = scaler::ymq::Logger(log_format, std::move(log_paths), scaler::ymq::Logger::stringToLogLevel(log_level));

    try {
        // NOTE: Setup IOSocket synchronously here because it is a one-time thing.
        const auto socketType {ymq::IOSocketType::Binder};
        _ioSocket = ymq::syncCreateSocket(_ioContext, socketType, std::move(identity));
        const std::string networkAddress {"tcp://" + name + ':' + port};
        ymq::syncBindSocket(_ioSocket, std::move(networkAddress));

        setServerReadyFd();

        _logger.log(scaler::ymq::Logger::LoggingLevel::info, "ObjectStorageServer: started");

        processRequests();

        _ioContext.removeIOSocket(_ioSocket);
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
    _ioContext.requestIOSocketStop(_ioSocket);
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

void ObjectStorageServer::processRequests()
{
    using namespace std::chrono_literals;
    Identity lastMessageIdentity;
    std::map<Identity, std::pair<ObjectRequestHeader, Bytes>> identityToFullRequest;
    while (true) {
        try {
            auto invalids = std::ranges::remove_if(_pendingSendMessageFuts, [](const auto& x) { return !x.valid(); });
            _pendingSendMessageFuts.erase(invalids.begin(), invalids.end());

            std::ranges::for_each(_pendingSendMessageFuts, [](auto& fut) {
                if (fut.wait_for(0s) == std::future_status::ready) {
                    auto error = fut.get();
                    assert(!error);
                }
            });

            auto maybeMessage = ymq::syncRecvMessage(_ioSocket);

            if (!maybeMessage) {
                auto error = maybeMessage.error();
                if (error._errorCode == ymq::Error::ErrorCode::IOSocketStopRequested) {
                    auto n = std::ranges::count_if(_pendingSendMessageFuts, [](auto& x) {
                        return x.valid() && x.wait_for(0s) == std::future_status::timeout;
                    });
                    if (!n) {
                        _logger.log(
                            scaler::ymq::Logger::LoggingLevel::info,
                            "ObjectStorageServer: stopped, number of messages leftover in the system = ",
                            std::ranges::count_if(_pendingSendMessageFuts, [](auto& x) {
                                return x.valid() && x.wait_for(0s) == std::future_status::timeout;
                            }));
                    }

                    if (pendingRequests.size()) {
                        _logger.log(
                            scaler::ymq::Logger::LoggingLevel::info,
                            "ObjectStorageServer: stopped, number of pending requests leftover in the system = ",
                            pendingRequests.size());
                        pendingRequests.clear();
                    }
                    return;
                } else {
                    throw error;
                }
            }

            const auto identity        = *maybeMessage->address.as_string();
            lastMessageIdentity        = identity;
            const auto headerOrPayload = std::move(maybeMessage->payload);

            auto it = identityToFullRequest.find(identity);
            if (it == identityToFullRequest.end()) {
                identityToFullRequest[identity].first = ObjectRequestHeader::fromBuffer(headerOrPayload);
                const auto& requestType               = identityToFullRequest[identity].first.requestType;
                if (requestType == ObjectRequestType::DUPLICATE_OBJECT_I_D ||
                    requestType == ObjectRequestType::SET_OBJECT) {
                    continue;
                }
            } else {
                assert(it->second.first.payloadLength == headerOrPayload.len());
                (it->second).second = std::move(headerOrPayload);
            }

            // NOTE: PostCondition of the previous statement: We have a full request here

            auto request = std::move(identityToFullRequest[identity]);
            identityToFullRequest.erase(identity);
            auto client = std::make_shared<Client>(_ioSocket, identity);

            switch (request.first.requestType) {
                case ObjectRequestType::SET_OBJECT: {
                    processSetRequest(std::move(client), std::move(request));
                    break;
                }
                case ObjectRequestType::GET_OBJECT: {
                    processGetRequest(std::move(client), request.first);
                    break;
                }
                case ObjectRequestType::DELETE_OBJECT: {
                    processDeleteRequest(client, request.first);
                    break;
                }
                case ObjectRequestType::DUPLICATE_OBJECT_I_D: {
                    processDuplicateRequest(client, request);
                    break;
                }
            }
        } catch (const kj::Exception& e) {
            _ioSocket->closeConnection(std::move(lastMessageIdentity));
            _logger.log(
                scaler::ymq::Logger::LoggingLevel::error,
                "ObjectStorageServer: Malformed capnp message. Connection closed, details: ",
                e.getDescription().cStr());
        } catch (const std::exception& e) {
            _logger.log(
                scaler::ymq::Logger::LoggingLevel::error, "ObjectStorageServer: unexpected error, reason: ", e.what());
        }
    }
}

void ObjectStorageServer::processSetRequest(
    std::shared_ptr<Client> client, std::pair<ObjectRequestHeader, Bytes> request)
{
    const auto requestHeader = std::move(request.first);
    auto requestPayload      = std::move(request.second);
    if (requestHeader.payloadLength > MEMORY_LIMIT_IN_BYTES) {
        throw std::runtime_error(
            "payload length is larger than MEMORY_LIMIT_IN_BYTES=" + std::to_string(MEMORY_LIMIT_IN_BYTES));
    }

    if (requestHeader.payloadLength > std::numeric_limits<size_t>::max()) {
        throw std::runtime_error("payload length is larger than SIZE_MAX=" + std::to_string(SIZE_MAX));
    }

    auto objectPtr = objectManager.setObject(requestHeader.objectID, std::move(requestPayload));

    optionallySendPendingRequests(requestHeader.objectID, objectPtr);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::SET_O_K,
    };

    writeMessage(client, responseHeader, {});
}

void ObjectStorageServer::processGetRequest(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader)
{
    auto objectPtr = objectManager.getObject(requestHeader.objectID);

    if (objectPtr != nullptr) {
        sendGetResponse(client, requestHeader, objectPtr);
        return;
    } else {
        // We don't have the object yet. Send the response later after once we receive the SET request.
        pendingRequests[requestHeader.objectID].emplace_back(client, requestHeader);
    }
}

void ObjectStorageServer::processDeleteRequest(std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader)
{
    bool success = objectManager.deleteObject(requestHeader.objectID);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = success ? ObjectResponseType::DEL_O_K : ObjectResponseType::DEL_NOT_EXISTS,
    };

    writeMessage(client, responseHeader, {});
}

void ObjectStorageServer::processDuplicateRequest(
    std::shared_ptr<Client> client, std::pair<ObjectRequestHeader, Bytes> request)
{
    auto requestHeader = std::move(request.first);
    auto payload       = std::move(request.second);
    if (requestHeader.payloadLength != ObjectID::bufferSize()) {
        throw std::runtime_error(
            "payload length should be size_of(ObjectID)=" + std::to_string(ObjectID::bufferSize()));
    }

    ObjectID originalObjectID = ObjectID::fromBuffer(payload);

    auto objectPtr = objectManager.duplicateObject(originalObjectID, requestHeader.objectID);

    std::vector<ObjectStorageServer::SendMessageFuture> res;
    if (objectPtr != nullptr) {
        optionallySendPendingRequests(requestHeader.objectID, objectPtr);
        sendDuplicateResponse(client, requestHeader);
    } else {
        // We don't have the referenced original object yet. Send the response later once we receive the SET
        pendingRequests[originalObjectID].emplace_back(client, requestHeader);
    }
}

void ObjectStorageServer::sendGetResponse(
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

    writeMessage(client, responseHeader, {objectPtr->data(), payloadLength});
}

void ObjectStorageServer::sendDuplicateResponse(
    std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader)
{
    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::DUPLICATE_O_K,
    };

    writeMessage(client, responseHeader, {});
}

void ObjectStorageServer::optionallySendPendingRequests(
    const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr)
{
    auto it = pendingRequests.find(objectID);
    if (it == pendingRequests.end()) {
        return;
    }

    // Immediately remove the object's pending requests, or else another coroutine might process them too.
    auto requests = std::move(it->second);
    pendingRequests.erase(it);

    std::vector<ObjectStorageServer::SendMessageFuture> res;
    for (auto& request: requests) {
        if (request.requestHeader.requestType == ObjectRequestType::GET_OBJECT) {
            sendGetResponse(request.client, request.requestHeader, objectPtr);
        } else {
            assert(request.requestHeader.requestType == ObjectRequestType::DUPLICATE_OBJECT_I_D);
            objectManager.duplicateObject(objectID, request.requestHeader.objectID);
            sendDuplicateResponse(request.client, request.requestHeader);

            // Some other pending requests might be themselves dependent on this duplicated object.
            optionallySendPendingRequests(request.requestHeader.objectID, objectPtr);
        }
    }
    return;
}

};  // namespace object_storage
};  // namespace scaler
