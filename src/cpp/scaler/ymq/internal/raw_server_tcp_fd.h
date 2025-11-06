#pragma once

#include <vector>

#include "scaler/ymq/internal/defs.h"  // system compatible header

namespace scaler {
namespace ymq {

class RawServerTCPFD {
public:
    RawServerTCPFD(sockaddr addr);

    RawServerTCPFD(const RawServerTCPFD&)            = delete;
    RawServerTCPFD(RawServerTCPFD&&)                 = delete;
    RawServerTCPFD& operator=(const RawServerTCPFD&) = delete;
    RawServerTCPFD& operator=(RawServerTCPFD&&)      = delete;

    ~RawServerTCPFD();
    void prepareAcceptSocket(void* notifyHandle);
    std::vector<std::pair<uint64_t, sockaddr>> getNewConns();

    bool setReuseAddress();
    void bindAndListen();
    auto nativeHandle() const noexcept { return (RawSocketType)_serverFD; }

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
