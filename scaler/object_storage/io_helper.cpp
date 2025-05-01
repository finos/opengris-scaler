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
#include <boost/asio/use_awaitable.hpp>
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
        std::size_t n =
            co_await boost::asio::async_read(socket, boost::asio::buffer(buf.data(), CAPNP_HEADER_SIZE), use_awaitable);

        capnp::FlatArrayMessageReader reader(
            kj::ArrayPtr<const capnp::word>((const capnp::word*)buf.data(), CAPNP_HEADER_SIZE / CAPNP_WORD_SIZE));
        auto requestRoot = reader.getRoot<::ObjectRequestHeader>();

        header.reqType       = requestRoot.getRequestType();
        header.payloadLength = requestRoot.getPayloadLength();

        auto objectID   = requestRoot.getObjectID();
        header.objectID = {
            objectID.getField0(),
            objectID.getField1(),
            objectID.getField2(),
            objectID.getField3(),
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
    switch (header.reqType) {
        case type::SET_OBJECT: break;

        case type::GET_OBJECT: co_return;
        case type::DELETE_OBJECT:
        default: header.payloadLength = 0; break;
    }

    if (header.payloadLength > MEMORY_LIMIT_IN_BYTES) {
        // Set header object id to null and send back
        header.objectID      = {0, 0, 0, 0};
        header.payloadLength = 0;
        co_return;
    }

    payload.resize(header.payloadLength);
    try {
        std::size_t n = co_await boost::asio::async_read(socket, boost::asio::buffer(payload), use_awaitable);
    } catch (boost::system::system_error& e) {
        printf("payload ends prematurely, e.what() = %s\n", e.what());
        throw e;
    }
}

boost::asio::awaitable<void> write_response_payload(
    boost::asio::ip::tcp::socket& socket, std::span<const unsigned char> payloadView) {
    try {
        co_await async_write(socket, boost::asio::buffer(payloadView.data(), payloadView.size()), use_awaitable);
    } catch (boost::system::system_error& e) {
        printf("write error e.what() = %s\n", e.what());
        throw e;
    }
}

boost::asio::awaitable<void> write_response_header(
    boost::asio::ip::tcp::socket& socket, ObjectResponseHeader& header, uint64_t payloadLength) {
    capnp::MallocMessageBuilder returnMsg;
    auto respRoot = returnMsg.initRoot<::ObjectResponseHeader>();
    respRoot.setResponseType(header.respType);
    respRoot.setPayloadLength(payloadLength);
    auto respRootObjectID = respRoot.initObjectID();
    respRootObjectID.setField0(header.objectID[0]);
    respRootObjectID.setField1(header.objectID[1]);
    respRootObjectID.setField2(header.objectID[2]);
    respRootObjectID.setField3(header.objectID[3]);

    auto buf = capnp::messageToFlatArray(returnMsg);
    try {
        co_await async_write(socket, boost::asio::buffer(buf.asBytes().begin(), buf.asBytes().size()), use_awaitable);
    } catch (boost::system::system_error& e) {
        printf("write error e.what() = %s\n", e.what());
        throw e;
    }
}

};  // namespace object_storage
};  // namespace scaler
