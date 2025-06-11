#pragma once

#include <span>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read.hpp>

#include "scaler/object_storage/defs.h"

namespace scaler {
namespace object_storage {

boost::asio::awaitable<void> read_request_header(boost::asio::ip::tcp::socket& socket, ObjectRequestHeader& header);

boost::asio::awaitable<void> read_request_payload(
    boost::asio::ip::tcp::socket& socket, ObjectRequestHeader& header, payload_t& payload);

boost::asio::awaitable<void> write_response_header(
    boost::asio::ip::tcp::socket& socket, ObjectResponseHeader& header, uint64_t payload_length);

boost::asio::awaitable<void> write_response_payload(
    boost::asio::ip::tcp::socket& socket, std::span<const unsigned char> payload_view);

};  // namespace object_storage
};  // namespace scaler
