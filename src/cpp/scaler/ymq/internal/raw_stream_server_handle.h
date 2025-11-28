#pragma once

#include <vector>

#include "scaler/ymq/internal/defs.h"  // system compatible header
#include "scaler/ymq/internal/socket_address.h"

namespace scaler {
namespace ymq {

class RawStreamServerHandle {
public:
    RawStreamServerHandle(SocketAddress address);

    RawStreamServerHandle(const RawStreamServerHandle&)            = delete;
    RawStreamServerHandle(RawStreamServerHandle&&)                 = delete;
    RawStreamServerHandle& operator=(const RawStreamServerHandle&) = delete;
    RawStreamServerHandle& operator=(RawStreamServerHandle&&)      = delete;

    ~RawStreamServerHandle();
    void prepareAcceptSocket(void* notifyHandle);
    std::vector<std::pair<uint64_t, SocketAddress>> getNewConns();

    bool setReuseAddress();
    void bindAndListen();
    auto nativeHandle() const noexcept { return (RawSocketType)_serverFD; }

    void destroy();

    socklen_t addrSize() const noexcept { return _address._addrLen; }

private:
    uint64_t _serverFD;
    SocketAddress _address;
#ifdef _WIN32
    uint64_t _newConn;
    LPFN_ACCEPTEX _acceptExFunc;
    char _buffer[128];
#endif  // _WIN32
};

}  // namespace ymq
}  // namespace scaler
