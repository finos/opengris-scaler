#pragma once

#include <vector>

#include "scaler/ymq/internal/defs.h"  // system compatible header

namespace scaler {
namespace ymq {

class RawStreamServerHandle {
public:
    RawStreamServerHandle(sockaddr addr);

    RawStreamServerHandle(const RawStreamServerHandle&)            = delete;
    RawStreamServerHandle(RawStreamServerHandle&&)                 = delete;
    RawStreamServerHandle& operator=(const RawStreamServerHandle&) = delete;
    RawStreamServerHandle& operator=(RawStreamServerHandle&&)      = delete;

    ~RawStreamServerHandle();
    void prepareAcceptSocket(void* notifyHandle);
    std::vector<std::pair<uint64_t, sockaddr>> getNewConns();

    bool setReuseAddress();
    void bindAndListen();
    auto nativeHandle() const noexcept { return (RawSocketType)_serverFD; }

    void destroy();

private:
    uint64_t _serverFD;
    sockaddr _addr;
#ifdef _WIN32
    uint64_t _newConn;
    LPFN_ACCEPTEX _acceptExFunc;
    char _buffer[128];
#endif  // _WIN32
};

}  // namespace ymq
}  // namespace scaler
