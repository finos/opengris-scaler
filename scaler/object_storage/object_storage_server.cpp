#include "scaler/object_storage/object_storage_server.h"

#include <algorithm>
#include <concepts>
#include <exception>
#include <future>

#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/error.h"
#include "scaler/io/ymq/simple_interface.h"
#include "scaler/io/ymq/typedefs.h"
#include "scaler/object_storage/message.h"

namespace scaler {
namespace object_storage {

ObjectStorageServer::ObjectStorageServer(): _stopped(false)
{
    initServerReadyFds();
}

ObjectStorageServer::~ObjectStorageServer()
{
    shutdown();
    closeServerReadyFds();
}

void ObjectStorageServer::run(
    std::string name,
    std::string port,
    scaler::ymq::Configuration::IOSocketIdentity identity,
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
    _stopped = true;
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

ObjectRequestHeader ObjectStorageServer::parseRequestHeader(Bytes message)
{
    assert(message.len() == CAPNP_HEADER_SIZE);
    std::array<uint64_t, ObjectRequestHeader::bufferSize() / CAPNP_WORD_SIZE> buffer {};
    memcpy(buffer.data(), message.data(), message.size());
    return ObjectRequestHeader::fromBuffer(buffer);
}

void ObjectStorageServer::processRequests()
{
    using namespace std::chrono_literals;
    try {
        std::map<ymq::Configuration::IOSocketIdentity, std::pair<ObjectRequestHeader, Bytes>> identityToFullRequest;
        std::vector<std::future<std::expected<void, ymq::Error>>> pendingSendMessageFuts;

        while (true) {
            // Try poll on sendMessage futures
            auto invalids = std::partition(
                pendingSendMessageFuts.begin(), pendingSendMessageFuts.end(), [](const auto& x) { return x.valid(); });
            pendingSendMessageFuts.resize(std::distance(pendingSendMessageFuts.begin(), invalids));

            std::ranges::for_each(pendingSendMessageFuts, [](auto& fut) {
                if (fut.wait_for(0s) == std::future_status::ready) {
                    auto res = fut.get();
                    assert(res);
                }
            });

            auto maybeMessageFut = ymq::futureRecvMessage(_ioSocket);
            while (maybeMessageFut.wait_for(100ms) == std::future_status::timeout) {
                if (_stopped) {
                    _logger.log(
                        scaler::ymq::Logger::LoggingLevel::info,
                        "ObjectStorageServer: stopped, number of messages leftover in the system = ",
                        std::ranges::count_if(pendingSendMessageFuts, [](auto& x) {
                            return x.valid() && x.wait_for(0s) == std::future_status::timeout;
                        }));
                    return;
                }
            }

            auto maybeMessage = maybeMessageFut.get();

            if (maybeMessage.second._errorCode != ymq::Error::ErrorCode::Uninit) {
                throw maybeMessage.second;
            }

            const ymq::Configuration::IOSocketIdentity identity = maybeMessage.first.address.as_string();
            const Bytes headerOrPayload                         = std::move(maybeMessage.first.payload);

            auto it = identityToFullRequest.find(identity);
            if (it == identityToFullRequest.end()) {
                identityToFullRequest[identity].first = parseRequestHeader(std::move(headerOrPayload));
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
                    catFuts(pendingSendMessageFuts, processSetRequest(std::move(client), std::move(request)));
                    break;
                }
                case ObjectRequestType::GET_OBJECT: {
                    catFuts(pendingSendMessageFuts, processGetRequest(std::move(client), request.first));
                    break;
                }
                case ObjectRequestType::DELETE_OBJECT: {
                    catFuts(pendingSendMessageFuts, processDeleteRequest(client, request.first));
                    break;
                }
                case ObjectRequestType::DUPLICATE_OBJECT_I_D: {
                    catFuts(pendingSendMessageFuts, processDuplicateRequest(client, request));
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
}

std::vector<ObjectStorageServer::SendMessageFuture> ObjectStorageServer::processSetRequest(
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

    auto futs = optionallySendPendingRequests(requestHeader.objectID, objectPtr);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::SET_O_K,
    };

    catFuts(futs, writeMessage(client, responseHeader, {}));
    return futs;
}

ObjectStorageServer::GetResponseResult ObjectStorageServer::processGetRequest(
    std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader)
{
    auto objectPtr = objectManager.getObject(requestHeader.objectID);

    if (objectPtr != nullptr) {
        return sendGetResponse(client, requestHeader, objectPtr);
    } else {
        // We don't have the object yet. Send the response later after once we receive the SET request.
        pendingRequests[requestHeader.objectID].emplace_back(client, requestHeader);
    }
    return {};
}

ObjectStorageServer::GetResponseResult ObjectStorageServer::processDeleteRequest(
    std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader)
{
    bool success = objectManager.deleteObject(requestHeader.objectID);

    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = success ? ObjectResponseType::DEL_O_K : ObjectResponseType::DEL_NOT_EXISTS,
    };

    return writeMessage(client, responseHeader, {});
}

std::vector<ObjectStorageServer::SendMessageFuture> ObjectStorageServer::processDuplicateRequest(
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
        catFuts(res, optionallySendPendingRequests(requestHeader.objectID, objectPtr));
        catFuts(res, sendDuplicateResponse(client, requestHeader));
    } else {
        // We don't have the referenced original object yet. Send the response later once we receive the SET
        pendingRequests[originalObjectID].emplace_back(client, requestHeader);
    }
    return res;
}

ObjectStorageServer::GetResponseResult ObjectStorageServer::sendGetResponse(
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

    return writeMessage(client, responseHeader, {objectPtr->data(), payloadLength});
}

ObjectStorageServer::GetResponseResult ObjectStorageServer::sendDuplicateResponse(
    std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader)
{
    ObjectResponseHeader responseHeader {
        .objectID      = requestHeader.objectID,
        .payloadLength = 0,
        .responseID    = requestHeader.requestID,
        .responseType  = ObjectResponseType::DUPLICATE_O_K,
    };

    return writeMessage(client, responseHeader, {});
}

std::vector<ObjectStorageServer::SendMessageFuture> ObjectStorageServer::optionallySendPendingRequests(
    const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr)
{
    auto it = pendingRequests.find(objectID);

    if (it == pendingRequests.end()) {
        return {};
    }

    // Immediately remove the object's pending requests, or else another coroutine might process them too.
    auto requests = std::move(it->second);
    pendingRequests.erase(it);

    std::vector<ObjectStorageServer::SendMessageFuture> res;
    for (auto& request: requests) {
        if (request.requestHeader.requestType == ObjectRequestType::GET_OBJECT) {
            catFuts(res, sendGetResponse(request.client, request.requestHeader, objectPtr));
        } else {
            assert(request.requestHeader.requestType == ObjectRequestType::DUPLICATE_OBJECT_I_D);
            objectManager.duplicateObject(objectID, request.requestHeader.objectID);
            catFuts(res, sendDuplicateResponse(request.client, request.requestHeader));

            // Some other pending requests might be themselves dependent on this duplicated object.
            catFuts(res, optionallySendPendingRequests(request.requestHeader.objectID, objectPtr));
        }
    }
    return res;
}

};  // namespace object_storage
};  // namespace scaler
