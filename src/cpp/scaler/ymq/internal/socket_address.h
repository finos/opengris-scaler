#pragma once
#include "scaler/ymq/internal/defs.h"

namespace scaler {
namespace ymq {

struct SocketAddress {
    enum class Type {
        IPC,
        TCP,
    };

    sockaddr_un _addr;
    socklen_t _addrLen;
    Type _type;
};

};  // namespace ymq
};  // namespace scaler
