#pragma once

#include <boost/asio/ip/tcp.hpp>

namespace scaler {
namespace object_storage {

int getAvailableTCPPort();

void setTCPNoDelay(boost::asio::ip::tcp::socket& socket, bool isNoDelay);

};  // namespace object_storage
};  // namespace scaler
