#pragma once

#include <cstdint>  // uint64_t
#include <memory>

#include "scaler/ymq/internal/socket_address.h"

struct sockaddr;
namespace scaler {
namespace ymq {

class RawStreamClientHandle {
public:
    RawStreamClientHandle(SocketAddress remoteAddr);
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

    uint64_t nativeHandle();

    bool isNetworkFD() const noexcept;

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

}  // namespace ymq
}  // namespace scaler
