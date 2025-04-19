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
        ObjectRequestHeader request_header;
        ObjectResponseHeader response_header;
    };

    struct object_with_meta {
        shared_object_t object;
        std::vector<meta> meta_info;
        // std::optional<meta> meta_info;
    };

    std::span<const unsigned char> get_memory_view_for_response_payload(
        scaler::object_storage::ObjectResponseHeader& header) {
        using type = ::ObjectResponseHeader::ObjectResponseType;
        switch (header.resp_type) {
            case type::GET_O_K: return {object_id_to_meta[header.object_id].object->data(), header.payload_length};
            case type::SET_O_K:
            case type::SET_O_K_OVERRIDE:
            case type::DEL_O_K:
            case type::DEL_NOT_EXISTS:
            default: break;
        }
        return {static_cast<const unsigned char*>(nullptr), 0};
    }

    bool update_record(
        const scaler::object_storage::ObjectRequestHeader& request_header,
        scaler::object_storage::ObjectResponseHeader& response_header,
        scaler::object_storage::payload_t payload) {
        using req_type            = ::ObjectRequestHeader::ObjectRequestType;
        using resp_type           = ::ObjectResponseHeader::ObjectResponseType;
        response_header.object_id = request_header.object_id;
        switch (request_header.req_type) {
            case req_type::SET_OBJECT:
                response_header.resp_type = object_id_to_meta[request_header.object_id].object ?
                                                resp_type::SET_O_K_OVERRIDE :
                                                resp_type::SET_O_K;
                object_id_to_meta[request_header.object_id].object =
                    std::make_shared<scaler::object_storage::object_t>(std::move(payload));
                for (auto& curr_meta: object_id_to_meta[request_header.object_id].meta_info) {
                    auto executor = curr_meta.socket.get_executor();
                    co_spawn(executor, write_once(std::move(curr_meta)), detached);
                }
                object_id_to_meta[request_header.object_id].meta_info = std::vector<meta>();

                break;

            case req_type::GET_OBJECT:
                response_header.resp_type = resp_type::GET_O_K;
                if (object_id_to_meta[request_header.object_id].object)
                    response_header.payload_length = object_id_to_meta[request_header.object_id].object->size();
                else
                    return false;
                break;

            case req_type::GET_OBJECT_HEADER:
                response_header.resp_type = resp_type::GET_O_K;
                if (object_id_to_meta[request_header.object_id].object)
                    response_header.payload_length = std::min(
                        request_header.payload_length, object_id_to_meta[request_header.object_id].object->size());
                else
                    return false;
                break;

            case req_type::DELETE_OBJECT:
                response_header.resp_type =
                    object_id_to_meta[request_header.object_id].object ? resp_type::DEL_O_K : resp_type::DEL_NOT_EXISTS;
                object_id_to_meta.erase(request_header.object_id);
                break;
        }
        return true;
    }

    awaitable<void> write_once(meta meta) {
        using type = ::ObjectRequestHeader::ObjectRequestType;
        if (meta.request_header.req_type == type::GET_OBJECT_HEADER) {
            meta.response_header.payload_length = std::min(
                object_id_to_meta[meta.response_header.object_id].object->size(), meta.request_header.payload_length);
        } else if (meta.request_header.req_type == type::GET_OBJECT) {
            meta.response_header.payload_length = object_id_to_meta[meta.response_header.object_id].object->size();
        }

        auto payload_view = get_memory_view_for_response_payload(meta.response_header);
        co_await scaler::object_storage::write_response_header(meta.socket, meta.response_header, payload_view.size());
        co_await scaler::object_storage::write_response_payload(meta.socket, payload_view);

        co_spawn(meta.socket.get_executor(), process_request(std::move(meta.socket)), detached);
    }

    std::map<scaler::object_storage::object_id_t, object_with_meta> object_id_to_meta;

public:
    awaitable<void> process_request(tcp::socket socket) {
        try {
            for (;;) {
                scaler::object_storage::ObjectRequestHeader requestHeader;
                co_await scaler::object_storage::read_request_header(socket, requestHeader);

                scaler::object_storage::payload_t payload;
                co_await scaler::object_storage::read_request_payload(socket, requestHeader, payload);

                scaler::object_storage::ObjectResponseHeader responseHeader;
                bool good_to_send = update_record(requestHeader, responseHeader, std::move(payload));

                if (!good_to_send) {
                    object_id_to_meta[requestHeader.object_id].meta_info.emplace_back(
                        std::move(socket), std::move(requestHeader), std::move(responseHeader));
                    break;
                }

                auto payload_view = get_memory_view_for_response_payload(responseHeader);

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
