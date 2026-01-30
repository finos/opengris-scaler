#pragma once

#include <string>
#include <variant>

#include "scaler/wrapper/uv/pipe.h"
#include "scaler/wrapper/uv/tcp.h"

namespace scaler {
namespace uv_ymq {

using Identity = std::string;

using Client = std::variant<scaler::wrapper::uv::TCPSocket, scaler::wrapper::uv::Pipe>;

}  // namespace uv_ymq
}  // namespace scaler
