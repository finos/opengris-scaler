#pragma once

#include <memory>
#include <vector>

#include "scaler/ymq/internal/socket_address.h"

struct sockaddr;
namespace scaler {
namespace ymq {

class RawStreamServerHandle {
public:
    RawStreamServerHandle(SocketAddress addr);

    RawStreamServerHandle(const RawStreamServerHandle&)            = delete;
    RawStreamServerHandle(RawStreamServerHandle&&)                 = delete;
    RawStreamServerHandle& operator=(const RawStreamServerHandle&) = delete;
    RawStreamServerHandle& operator=(RawStreamServerHandle&&)      = delete;

    ~RawStreamServerHandle();
    void prepareAcceptSocket(void* notifyHandle);
    std::vector<std::pair<uint64_t, SocketAddress>> getNewConns();

    bool setReuseAddress();
    void bindAndListen();
    uint64_t nativeHandle();

    void destroy();

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;
};

}  // namespace ymq
}  // namespace scaler
