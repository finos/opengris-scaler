#pragma once

#include <cstdint>  // uint64_t

#include "scaler/ymq/internal/defs.h"

namespace scaler {
namespace ymq {

class RawClientTCPFD {
public:
    RawClientTCPFD(sockaddr remoteAddr);
    ~RawClientTCPFD();

    RawClientTCPFD(RawClientTCPFD&&)                  = delete;
    RawClientTCPFD& operator=(RawClientTCPFD&& other) = delete;
    RawClientTCPFD(const RawClientTCPFD&)             = delete;
    RawClientTCPFD& operator=(const RawClientTCPFD&)  = delete;

    void create();
    void destroy();
    bool prepConnect(void* notifyHandle);
    bool needRetry();

    void zeroNativeHandle() noexcept;

    auto nativeHandle() const noexcept { return (RawSocketType)_clientFD; }

private:
    uint64_t _clientFD;
    sockaddr _remoteAddr;
#ifdef _WIN32
    LPFN_CONNECTEX _connectExFunc;
#endif  // _WIN32
};

}  // namespace ymq
}  // namespace scaler
