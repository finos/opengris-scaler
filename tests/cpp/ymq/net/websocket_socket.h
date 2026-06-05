#pragma once
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "scaler/ymq/address.h"
#include "tests/cpp/ymq/net/socket.h"

class WebSocketSocket: public Socket {
public:
    WebSocketSocket();
    ~WebSocketSocket();

    WebSocketSocket(WebSocketSocket&&) noexcept;
    WebSocketSocket& operator=(WebSocketSocket&&) noexcept;
    WebSocketSocket(const WebSocketSocket&)            = delete;
    WebSocketSocket& operator=(const WebSocketSocket&) = delete;

    void tryConnect(const scaler::ymq::Address& address, int tries = 10) const override;
    void bind(const scaler::ymq::Address& address) const override;
    void listen(int backlog = 5) const override;
    std::unique_ptr<Socket> accept() const override;

    void writeAll(const void* data, size_t size) const override;
    void writeAll(std::string msg) const override;

    void readExact(void* buffer, size_t size) const override;

    void writeMessage(std::string msg) const override;

    std::string readMessage() const override;

private:
    long long _fd;
    bool _isServer;
    mutable std::vector<uint8_t> _recvBuffer;

    WebSocketSocket(long long fd, bool isServer);

    long long rawAcceptFd() const;
    int rawRead(void* buffer, size_t size) const;
    int rawWrite(const void* buffer, size_t size) const;

    void rawWriteAll(const void* data, size_t size) const;
    void rawReadExact(void* buffer, size_t size) const;

    void sendFrame(const void* data, size_t size) const;
    void fillRecvBuffer(size_t needed) const;

    void performClientHandshake(const scaler::ymq::WebSocketAddress& address) const;
    void performServerHandshake() const;
};
