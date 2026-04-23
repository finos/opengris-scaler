#include <Windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>

#include <stdexcept>
#include <thread>

#include "scaler/ymq/address.h"
#include "tests/cpp/ymq/common/utils.h"
#include "tests/cpp/ymq/net/websocket_socket.h"

WebSocketSocket::WebSocketSocket(): _fd(-1), _isServer(false)
{
    _fd = static_cast<long long>(::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP));
    if (_fd == SOCKET_ERROR)
        raiseSocketError("failed to create socket");
}

WebSocketSocket::~WebSocketSocket()
{
    ::closesocket(static_cast<SOCKET>(_fd));
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
        if (::connect(static_cast<SOCKET>(_fd), addr, sizeof(sockaddr_in)) != SOCKET_ERROR)
            break;

        if (WSAGetLastError() == WSAECONNREFUSED) {
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
    if (::setsockopt(
            static_cast<SOCKET>(_fd),
            SOL_SOCKET,
            SO_REUSEADDR,
            reinterpret_cast<const char*>(&optval),
            sizeof(optval)) == SOCKET_ERROR)
        raiseSocketError("failed to set SO_REUSEADDR");

    const sockaddr* addr = address.asWebSocket().tcpAddress.toSockAddr();
    if (::bind(static_cast<SOCKET>(_fd), addr, sizeof(sockaddr_in)) == SOCKET_ERROR)
        raiseSocketError("failed to bind");
}

void WebSocketSocket::listen(int backlog) const
{
    if (::listen(static_cast<SOCKET>(_fd), backlog) == SOCKET_ERROR)
        raiseSocketError("failed to listen");
}

long long WebSocketSocket::rawAcceptFd() const
{
    const long long fd = static_cast<long long>(::accept(static_cast<SOCKET>(_fd), nullptr, nullptr));
    if (fd == SOCKET_ERROR)
        raiseSocketError("failed to accept");
    return fd;
}

int WebSocketSocket::rawRead(void* buffer, size_t size) const
{
    const int n = ::recv(static_cast<SOCKET>(_fd), static_cast<char*>(buffer), static_cast<int>(size), 0);
    if (n == SOCKET_ERROR)
        raiseSocketError("failed to receive");
    return n;
}

int WebSocketSocket::rawWrite(const void* buffer, size_t size) const
{
    const int n = ::send(static_cast<SOCKET>(_fd), static_cast<const char*>(buffer), static_cast<int>(size), 0);
    if (n == SOCKET_ERROR)
        raiseSocketError("failed to send");
    return n;
}
