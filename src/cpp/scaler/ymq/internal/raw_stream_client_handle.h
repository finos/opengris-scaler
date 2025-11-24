#pragma once

#include <cstdint>  // uint64_t

#include "scaler/ymq/internal/defs.h"

namespace scaler {
namespace ymq {

class RawStreamClientHandle {
public:
    RawStreamClientHandle(sockaddr remoteAddr);
    ~RawStreamClientHandle();

    RawStreamClientHandle(RawStreamClientHandle&&)                  = delete;
    RawStreamClientHandle& operator=(RawStreamClientHandle&& other) = delete;
    RawStreamClientHandle(const RawStreamClientHandle&)             = delete;
    RawStreamClientHandle& operator=(const RawStreamClientHandle&)  = delete;

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
