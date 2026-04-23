#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <stdexcept>
#include <thread>

#include "scaler/ymq/address.h"
#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/net/websocket_socket.h"

WebSocketSocket::WebSocketSocket(): _fd(-1), _isServer(false)
{
    _fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (_fd < 0)
        raiseSocketError("failed to create socket");
}

WebSocketSocket::~WebSocketSocket()
{
    ::close(static_cast<int>(_fd));
}

WebSocketSocket::WebSocketSocket(WebSocketSocket&& other) noexcept
    : _fd(other._fd), _isServer(other._isServer), _recvBuffer(std::move(other._recvBuffer))
{
    other._fd = -1;
}

WebSocketSocket& WebSocketSocket::operator=(WebSocketSocket&& other) noexcept
{
    _fd         = other._fd;
    _isServer   = other._isServer;
    _recvBuffer = std::move(other._recvBuffer);
    other._fd   = -1;
    return *this;
}

void WebSocketSocket::tryConnect(const scaler::ymq::Address& address, int tries) const
{
    if (address.type() != scaler::ymq::Address::Type::WebSocket)
        throw std::runtime_error("Unsupported protocol for WebSocketSocket: expected WebSocket");

    const scaler::ymq::WebSocketAddress& ws = address.asWebSocket();
    const sockaddr* addr                    = ws.tcpAddress.toSockAddr();

    for (int i = 0; i < tries; ++i) {
        if (::connect(static_cast<int>(_fd), addr, sizeof(sockaddr_in)) == 0)
            break;

        if (errno == ECONNREFUSED) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        }

        raiseSocketError("failed to connect");
    }

    performClientHandshake(ws);
}

void WebSocketSocket::bind(const scaler::ymq::Address& address) const
{
    if (address.type() != scaler::ymq::Address::Type::WebSocket)
        throw std::runtime_error("Unsupported protocol for WebSocketSocket: expected WebSocket");

    int optval = 1;
    if (::setsockopt(static_cast<int>(_fd), SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0)
        raiseSocketError("failed to set SO_REUSEADDR");

    const sockaddr* addr = address.asWebSocket().tcpAddress.toSockAddr();
    if (::bind(static_cast<int>(_fd), addr, sizeof(sockaddr_in)) < 0)
        raiseSocketError("failed to bind");
}

void WebSocketSocket::listen(int backlog) const
{
    if (::listen(static_cast<int>(_fd), backlog) < 0)
        raiseSocketError("failed to listen");
}

long long WebSocketSocket::rawAcceptFd() const
{
    const long long fd = ::accept(static_cast<int>(_fd), nullptr, nullptr);
    if (fd < 0)
        raiseSocketError("failed to accept");
    return fd;
}

int WebSocketSocket::rawRead(void* buffer, size_t size) const
{
    const int n = static_cast<int>(::read(static_cast<int>(_fd), buffer, size));
    if (n < 0)
        raiseSocketError("failed to recv");
    return n;
}

int WebSocketSocket::rawWrite(const void* buffer, size_t size) const
{
    const int n = static_cast<int>(::write(static_cast<int>(_fd), buffer, size));
    if (n < 0)
        raiseSocketError("failed to send");
    return n;
}
