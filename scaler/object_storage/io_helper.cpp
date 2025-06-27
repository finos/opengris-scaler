#include "io_helper.h"

#include <capnp/message.h>
#include <capnp/serialize.h>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/write.hpp>
#include <boost/system/system_error.hpp>
#include <exception>
#include <iostream>

#include "protocol/object_storage.capnp.h"
#include "scaler/object_storage/constants.h"
#include "scaler/object_storage/defs.h"

using boost::asio::awaitable;
using boost::asio::use_awaitable;
using boost::asio::ip::tcp;

namespace scaler {
namespace object_storage {

void setTCPNoDelay(tcp::socket& socket, bool is_no_delay) {
    boost::system::error_code ec;
    socket.set_option(tcp::no_delay(is_no_delay), ec);

    if (ec) {
        std::cerr << "failed to set TCP_NODELAY on client socket: " << ec.message() << std::endl;
        std::terminate();
    }
}

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
        header.requestID     = requestRoot.getRequestID();

        auto objectID   = requestRoot.getObjectID();
        header.objectID = {
            objectID.getField0(),
            objectID.getField1(),
            objectID.getField2(),
            objectID.getField3(),
        };
    } catch (boost::system::system_error& e) {
        // TODO: make this a log, since eof is not really an err.
        if (e.code() == boost::asio::error::eof) {
            std::cerr << "Remote end closed, nothing to read.\n";
        } else {
            std::cerr << "exception throwned, read error e.what() = " << e.what() << '\n';
        }
        throw e;
    } catch (std::exception& e) {
        // TODO: make this a log, capnp header corruption is an err.
        std::cerr << "exception throwned, header not a capnp e.what() = " << e.what() << '\n';

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
        std::cerr << "payload ends prematurely, e.what() = " << e.what() << '\n';
        std::cerr << "Failing fast. Terminting now...\n";
        std::terminate();
    }
}

boost::asio::awaitable<void> write_response(
    boost::asio::ip::tcp::socket& socket, ObjectResponseHeader& header, std::span<const unsigned char> payload) {
    capnp::MallocMessageBuilder returnMsg;
    auto respRoot = returnMsg.initRoot<::ObjectResponseHeader>();
    respRoot.setResponseType(header.respType);
    respRoot.setPayloadLength(payload.size());
    respRoot.setResponseID(header.responseID);
    auto respRootObjectID = respRoot.initObjectID();
    respRootObjectID.setField0(header.objectID[0]);
    respRootObjectID.setField1(header.objectID[1]);
    respRootObjectID.setField2(header.objectID[2]);
    respRootObjectID.setField3(header.objectID[3]);

    auto msgBuf = capnp::messageToFlatArray(returnMsg);

    std::array<boost::asio::const_buffer, 2> buffers {
        boost::asio::buffer(msgBuf.asBytes().begin(), msgBuf.asBytes().size()),
        boost::asio::buffer(payload),
    };

    try {
        co_await async_write(socket, buffers, use_awaitable);
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

};  // namespace object_storage
};  // namespace scaler
