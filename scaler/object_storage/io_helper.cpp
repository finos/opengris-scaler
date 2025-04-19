#include "io_helper.h"

#include <capnp/message.h>
#include <capnp/serialize.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/system_error.hpp>

#include "constants.h"
#include "defs.h"
#include "protocol/object_storage.capnp.h"

using boost::asio::awaitable;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;

namespace scaler {
namespace object_storage {

awaitable<void> read_request_header(tcp::socket& socket, ObjectRequestHeader& header) {
    try {
        std::array<uint64_t, CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE> buf;
        std::size_t n = co_await boost::asio::async_read(socket, boost::asio::buffer(buf.data(), CAPNP_HEADER_SIZE));

        capnp::FlatArrayMessageReader reader(
            kj::ArrayPtr<const capnp::word>((const capnp::word*)buf.data(), CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE));
        auto request_root = reader.getRoot<::ObjectRequestHeader>();

        header.req_type       = request_root.getRequestType();
        header.payload_length = request_root.getPayloadLength();

        auto object_id   = request_root.getObjectID();
        header.object_id = {
            object_id.getField0(),
            object_id.getField1(),
            object_id.getField2(),
            object_id.getField3(),
        };
    } catch (boost::system::system_error& e) {
        // TODO: make this a log, since eof is not really an err.
        printf("exception throwned, read error e.what() = %s\n", e.what());
        throw e;
    } catch (std::exception& e) {
        // TODO: make this a log, capnp header corruption is an err.
        printf("exception throwned, header not a capnp e.what() = %s\n", e.what());
        throw e;
    }
}

awaitable<void> read_request_payload(tcp::socket& socket, ObjectRequestHeader& header, payload_t& payload) {
    using type = ::ObjectRequestHeader::ObjectRequestType;
    switch (header.req_type) {
        case type::SET_OBJECT: break;

        case type::GET_OBJECT_HEADER: co_return;
        case type::DELETE_OBJECT:
        case type::GET_OBJECT:
        default: header.payload_length = 0; break;
    }

    if (header.payload_length > MEMORY_LIMIT_IN_BYTES) {
        // Set header object id to null and send back
        header.object_id      = {0, 0, 0, 0};
        header.payload_length = 0;
        co_return;
    }

    payload.resize(header.payload_length);
    try {
        std::size_t n = co_await boost::asio::async_read(socket, boost::asio::buffer(payload));
    } catch (boost::system::system_error& e) {
        printf("payload ends prematurely, e.what() = %s\n", e.what());
        throw e;
    }
}

boost::asio::awaitable<void> write_response_payload(
    boost::asio::ip::tcp::socket& socket, std::span<const unsigned char> payload_view) {
    try {
        co_await async_write(socket, boost::asio::buffer(payload_view.data(), payload_view.size()), use_awaitable);
    } catch (boost::system::system_error& e) {
        printf("write error e.what() = %s\n", e.what());
        throw e;
    }
}

boost::asio::awaitable<void> write_response_header(
    boost::asio::ip::tcp::socket& socket, ObjectResponseHeader& header, uint64_t payload_length) {
    capnp::MallocMessageBuilder return_msg;
    auto resp_root = return_msg.initRoot<::ObjectResponseHeader>();
    resp_root.setResponseType(header.resp_type);
    resp_root.setPayloadLength(payload_length);
    auto resp_root_object_id = resp_root.initObjectID();
    resp_root_object_id.setField0(header.object_id[0]);
    resp_root_object_id.setField1(header.object_id[1]);
    resp_root_object_id.setField2(header.object_id[2]);
    resp_root_object_id.setField3(header.object_id[3]);

    auto buf = capnp::messageToFlatArray(return_msg);
    try {
        co_await async_write(socket, boost::asio::buffer(buf.asBytes().begin(), buf.asBytes().size()), use_awaitable);
    } catch (boost::system::system_error& e) {
        printf("write error e.what() = %s\n", e.what());
        throw e;
    }
}

};  // namespace object_storage
};  // namespace scaler
