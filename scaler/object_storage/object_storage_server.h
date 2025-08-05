#pragma once

#include <boost/asio.hpp>
#include <iostream>
#include <memory>
#include <span>

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
    ObjectStorageServer();

    ~ObjectStorageServer();

    void run(std::string name, std::string port);

    void waitUntilReady();

    void shutdown();

private:
    struct Client {
        Client(boost::asio::any_io_executor executor): socket(executor), writeStrand(executor) {}

        tcp::socket socket;

        // As multiple detached coroutines can concurrently write to the client's socket (because of
        // `optionallySendPendingRequests()`), we must ensure that the calls to `async_write()` are sequenced in the
        // same strand (i.e. an asio execution queue).
        // As all `async_read()` calls are made from a single coroutine, we don't need to protect these.
        // See https://www.boost.org/doc/libs/latest/doc/html/boost_asio/reference/async_write/overload1.html.
        boost::asio::strand<boost::asio::any_io_executor> writeStrand;
    };

    struct PendingRequest {
        std::shared_ptr<Client> client;
        ObjectRequestHeader requestHeader;
    };

    using ObjectRequestType  = scaler::protocol::ObjectRequestHeader::ObjectRequestType;
    using ObjectResponseType = scaler::protocol::ObjectResponseHeader::ObjectResponseType;

    boost::asio::io_context ioContext {1};

    int onServerReadyReader;
    int onServerReadyWriter;

    ObjectManager objectManager;

    // Some GET and DUPLICATE requests might be delayed if the referenced object isn't available yet.
    std::map<ObjectID, std::vector<PendingRequest>> pendingRequests;

    void initServerReadyFds();

    void setServerReadyFd();

    void closeServerReadyFds();

    awaitable<void> listener(tcp::endpoint endpoint);

    awaitable<void> processRequests(std::shared_ptr<Client> client);

    awaitable<void> processSetRequest(std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader);

    awaitable<void> processGetRequest(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader);

    awaitable<void> processDeleteRequest(std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader);

    awaitable<void> processDuplicateRequest(std::shared_ptr<Client> client, ObjectRequestHeader& requestHeader);

    template <ObjectStorageMessage T>
    awaitable<T> readMessage(std::shared_ptr<Client> client)
    {
        try {
            std::array<uint64_t, T::bufferSize() / CAPNP_WORD_SIZE> buffer;
            co_await boost::asio::async_read(
                client->socket, boost::asio::buffer(buffer.data(), T::bufferSize()), use_awaitable);

            co_return T::fromBuffer(buffer);
        } catch (boost::system::system_error& e) {
            // TODO: make this a log, since eof is not really an err.
            if (e.code() == boost::asio::error::eof) {
                std::cerr << "Remote end closed, nothing to read.\n";
            } else {
                std::cerr << "exception thrown, read error e.what() = " << e.what() << '\n';
            }
            throw e;
        } catch (std::exception& e) {
            // TODO: make this a log, capnp header corruption is an err.
            std::cerr << "exception thrown, message not a capnp e.what() = " << e.what() << '\n';
            throw e;
        }
    }

    template <ObjectStorageMessage T>
    boost::asio::awaitable<void> writeMessage(
        std::shared_ptr<Client> client, T& message, std::span<const unsigned char> payload)
    {
        auto messageBuffer = message.toBuffer();

        std::array<boost::asio::const_buffer, 2> buffers {
            boost::asio::buffer(messageBuffer.asBytes().begin(), messageBuffer.asBytes().size()),
            boost::asio::buffer(payload),
        };

        try {
            co_await boost::asio::async_write(
                client->socket, buffers, boost::asio::bind_executor(client->writeStrand, boost::asio::use_awaitable));

        } catch (boost::system::system_error& e) {
            // TODO: Log support
            if (e.code() == boost::asio::error::broken_pipe) {
                std::cerr << "Remote end closed, nothing to write.\n";
                std::cerr << "This should never happen as the client is expected "
                          << "to get every and all response. Terminating now...\n";
                std::terminate();
            } else {
                std::cerr << "write error e.what() = " << e.what() << '\n';
            }
            throw e;
        }
    }

    awaitable<void> sendGetResponse(
        std::shared_ptr<Client> client,
        const ObjectRequestHeader& requestHeader,
        std::shared_ptr<const ObjectPayload> objectPtr);

    awaitable<void> sendDuplicateResponse(std::shared_ptr<Client> client, const ObjectRequestHeader& requestHeader);

    awaitable<void> optionallySendPendingRequests(
        const ObjectID& objectID, std::shared_ptr<const ObjectPayload> objectPtr);
};

};  // namespace object_storage
};  // namespace scaler
