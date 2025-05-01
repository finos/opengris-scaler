#pragma once

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <map>

#include "defs.h"
#include "io_helper.h"
#include "protocol/object_storage.capnp.h"

namespace scaler {
namespace object_storage {

using boost::asio::awaitable;
using boost::asio::co_spawn;
using boost::asio::detached;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;

class ObjectStorageServer {
    struct meta {
        boost::asio::ip::tcp::socket socket;
        ObjectRequestHeader requestHeader;
        ObjectResponseHeader responseHeader;
    };

    struct object_with_meta {
        shared_object_t object;
        std::vector<meta> metaInfo;
    };

    using reqType  = ::ObjectRequestHeader::ObjectRequestType;
    using respType = ::ObjectResponseHeader::ObjectResponseType;

    std::span<const unsigned char> getMemoryViewForResponsePayload(
        scaler::object_storage::ObjectResponseHeader& header) {
        switch (header.respType) {
            case respType::GET_O_K: return {objectIDToMeta[header.objectID].object->data(), header.payloadLength};
            case respType::SET_O_K:
            case respType::SET_O_K_OVERRIDE:
            case respType::DEL_O_K:
            case respType::DEL_NOT_EXISTS:
            default: break;
        }
        return {static_cast<const unsigned char*>(nullptr), 0};
    }

#ifndef NDEBUG
public:
#endif
    bool updateRecord(
        const scaler::object_storage::ObjectRequestHeader& requestHeader,
        scaler::object_storage::ObjectResponseHeader& responseHeader,
        scaler::object_storage::payload_t payload) {
        responseHeader.objectID = requestHeader.objectID;
        switch (requestHeader.reqType) {
            case reqType::SET_OBJECT:
                responseHeader.respType =
                    objectIDToMeta[requestHeader.objectID].object ? respType::SET_O_K_OVERRIDE : respType::SET_O_K;
                objectIDToMeta[requestHeader.objectID].object =
                    std::make_shared<scaler::object_storage::object_t>(std::move(payload));
                break;

            case reqType::GET_OBJECT:
                responseHeader.respType = respType::GET_O_K;
                if (objectIDToMeta[requestHeader.objectID].object)
                    responseHeader.payloadLength =
                        std::min(objectIDToMeta[requestHeader.objectID].object->size(), requestHeader.payloadLength);
                else
                    return false;
                break;

            case reqType::DELETE_OBJECT:
                responseHeader.respType =
                    objectIDToMeta[requestHeader.objectID].object ? respType::DEL_O_K : respType::DEL_NOT_EXISTS;
                objectIDToMeta.erase(requestHeader.objectID);
                break;
        }
        return true;
    }

private:
    awaitable<void> write_once(meta meta) {
        if (meta.requestHeader.reqType == reqType::GET_OBJECT) {
            meta.responseHeader.payloadLength =
                std::min(objectIDToMeta[meta.responseHeader.objectID].object->size(), meta.requestHeader.payloadLength);
        }

        auto payload_view = getMemoryViewForResponsePayload(meta.responseHeader);
        co_await scaler::object_storage::write_response_header(meta.socket, meta.responseHeader, payload_view.size());
        co_await scaler::object_storage::write_response_payload(meta.socket, payload_view);

        co_spawn(meta.socket.get_executor(), process_request(std::move(meta.socket)), detached);
    }

    void optionally_send_pending_requests(scaler::object_storage::ObjectRequestHeader requestHeader) {
        if (requestHeader.reqType == reqType::SET_OBJECT) {
            for (auto& curr_meta: objectIDToMeta[requestHeader.objectID].metaInfo) {
                auto executor = curr_meta.socket.get_executor();
                co_spawn(executor, write_once(std::move(curr_meta)), detached);
            }
            objectIDToMeta[requestHeader.objectID].metaInfo = std::vector<meta>();
        }
    }

#ifndef NDEBUG
public:
#endif
    std::map<scaler::object_storage::object_id_t, object_with_meta> objectIDToMeta;

public:
    awaitable<void> process_request(tcp::socket socket) {
        try {
            for (;;) {
                scaler::object_storage::ObjectRequestHeader requestHeader;
                co_await scaler::object_storage::read_request_header(socket, requestHeader);

                scaler::object_storage::payload_t payload;
                co_await scaler::object_storage::read_request_payload(socket, requestHeader, payload);

                scaler::object_storage::ObjectResponseHeader responseHeader;
                bool good_to_send = updateRecord(requestHeader, responseHeader, std::move(payload));

                optionally_send_pending_requests(requestHeader);

                if (!good_to_send) {
                    objectIDToMeta[requestHeader.objectID].metaInfo.emplace_back(
                        std::move(socket), std::move(requestHeader), std::move(responseHeader));
                    break;
                }

                auto payload_view = getMemoryViewForResponsePayload(responseHeader);

                co_await scaler::object_storage::write_response_header(socket, responseHeader, payload_view.size());

                co_await scaler::object_storage::write_response_payload(socket, payload_view);
            }
        } catch (std::exception& e) {
            // std::printf("process_request Exception: %s\n", e.what());
        }
    }
};

};  // namespace object_storage
};  // namespace scaler
