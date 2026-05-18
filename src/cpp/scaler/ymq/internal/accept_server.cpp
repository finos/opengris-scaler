#include "scaler/ymq/internal/accept_server.h"

#include <uv.h>

#include <atomic>
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <functional>
#include <utility>

#define YMQDIAG(...)                                  \
    do {                                              \
        std::fprintf(stderr, "[YMQDIAG] " __VA_ARGS__); \
        std::fputc('\n', stderr);                     \
        std::fflush(stderr);                          \
    } while (0)

#include "scaler/wrapper/uv/pipe.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/internal/websocket_stream.h"

namespace scaler {
namespace ymq {
namespace internal {

AcceptServer::AcceptServer(
    scaler::wrapper::uv::Loop& loop, Address address, ConnectionCallback onConnectionCallback) noexcept
{
    std::optional<Server> server;
    std::optional<WebSocketAddress> webSocketAddress;

    switch (address.type()) {
        case Address::Type::TCP: {
            auto tcpServer = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop));
            UV_EXIT_ON_ERROR(tcpServer.bind(address.asTCP(), uv_tcp_flags(0)));
            server = std::move(tcpServer);
            break;
        }
        case Address::Type::IPC: {
            auto pipeServer = UV_EXIT_ON_ERROR(scaler::wrapper::uv::PipeServer::init(loop, false));
            UV_EXIT_ON_ERROR(pipeServer.bind(address.asIPC()));
            server = std::move(pipeServer);
            break;
        }
        case Address::Type::WebSocket: {
            // WebSocket runs over TCP; bind a TCPServer to the resolved TCP address.
            webSocketAddress = address.asWebSocket();
            auto tcpServer   = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPServer::init(loop));
            UV_EXIT_ON_ERROR(tcpServer.bind(webSocketAddress->tcpAddress, uv_tcp_flags(0)));
            server = std::move(tcpServer);
            break;
        }
        default: std::unreachable();
    }

    _state = std::make_shared<State>(
        loop, std::move(onConnectionCallback), std::move(server.value()), std::move(webSocketAddress));

    std::string diagAddr = address.toString().value_or("<addr?>");
    if (auto* tcpServer = std::get_if<scaler::wrapper::uv::TCPServer>(&_state->_server.value())) {
        UV_EXIT_ON_ERROR(tcpServer->listen(serverListenBacklog, std::bind_front(&AcceptServer::onConnection, _state)));
        YMQDIAG("AcceptServer LISTEN tcp %s backlog=%d", diagAddr.c_str(), serverListenBacklog);
    } else if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&_state->_server.value())) {
        UV_EXIT_ON_ERROR(pipeServer->listen(serverListenBacklog, std::bind_front(&AcceptServer::onConnection, _state)));
        YMQDIAG("AcceptServer LISTEN ipc %s backlog=%d", diagAddr.c_str(), serverListenBacklog);
    } else {
        std::unreachable();
    }
}

AcceptServer::State::State(
    scaler::wrapper::uv::Loop& loop,
    ConnectionCallback onConnectionCallback,
    Server server,
    std::optional<WebSocketAddress> webSocketAddress) noexcept
    : _loop(loop)
    , _onConnectionCallback(std::move(onConnectionCallback))
    , _server(std::move(server))
    , _webSocketAddress(std::move(webSocketAddress))
{
}

AcceptServer::~AcceptServer() noexcept
{
    if (_state == nullptr) {
        return;  // instance moved
    }

    disconnect();
}

Address AcceptServer::address() const noexcept
{
    if (auto* tcpServer = std::get_if<scaler::wrapper::uv::TCPServer>(&_state->_server.value())) {
        const scaler::wrapper::uv::SocketAddress actualAddr = UV_EXIT_ON_ERROR(tcpServer->getSockName());

        if (_state->_webSocketAddress.has_value()) {
            // Reconstruct the WebSocket address with the actual bound port (handles port 0 auto-assignment).
            WebSocketAddress reconstructed = _state->_webSocketAddress.value();
            reconstructed.port             = static_cast<uint16_t>(actualAddr.port());
            reconstructed.tcpAddress       = actualAddr;
            return Address {std::move(reconstructed)};
        }

        return Address {actualAddr};
    } else if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&_state->_server.value())) {
        return Address {UV_EXIT_ON_ERROR(pipeServer->getSockName())};
    } else {
        std::unreachable();
    }
}

void AcceptServer::disconnect() noexcept
{
    if (!_state->_server.has_value()) {
        return;
    }

    std::optional<std::string> pipeName {};
    if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&_state->_server.value())) {
        pipeName = UV_EXIT_ON_ERROR(pipeServer->getSockName());
    }

    YMQDIAG("AcceptServer DISCONNECT (stop listening)");
    _state->_server = std::nullopt;

    if (pipeName.has_value()) {
        // libuv does not remove the pipe file on POSIX. On Windows the path is a Windows named pipe
        // (\\.\pipe\<name>) which has no filesystem entry to remove, and std::filesystem::remove
        // would throw inside this noexcept function. Use the non-throwing overload.
        std::error_code ec;
        std::filesystem::remove(pipeName.value(), ec);
    }
}

void AcceptServer::onConnection(
    std::shared_ptr<State> state, std::expected<void, scaler::wrapper::uv::Error> result) noexcept
{
    static std::atomic<uint64_t> s_acceptSeq {0};

    if (!result.has_value()) {
        YMQDIAG("AcceptServer onConnection ERROR uv=%d (about to UV_EXIT_ON_ERROR)", result.error().code());
    }
    UV_EXIT_ON_ERROR(result);

    if (state->_server == std::nullopt) {
        YMQDIAG("AcceptServer onConnection FIRED but server=nullopt (disconnecting) -- connection dropped");
        return;  // server disconnecting
    }

    if (auto* tcpServer = std::get_if<scaler::wrapper::uv::TCPServer>(&state->_server.value())) {
        int diagPort = -1;
        if (auto sn = tcpServer->getSockName(); sn.has_value()) {
            diagPort = static_cast<int>(sn.value().port());
        }
        uint64_t seq = ++s_acceptSeq;
        YMQDIAG("AcceptServer onConnection ENTER tcp port=%d (totalAccepts=%llu)", diagPort, (unsigned long long)seq);
        scaler::wrapper::uv::TCPSocket tcpClient = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(state->_loop));
        UV_EXIT_ON_ERROR(tcpServer->accept(tcpClient));
        YMQDIAG("AcceptServer ACCEPTED tcp port=%d seq=%llu", diagPort, (unsigned long long)seq);

        if (state->_webSocketAddress.has_value()) {
            WebSocketStream::upgradeAsServer(
                std::move(tcpClient),
                [state](std::expected<WebSocketStream, scaler::wrapper::uv::Error> wsResult) mutable {
                    if (!wsResult.has_value()) {
                        // Reject this connection silently; the server keeps running.
                        return;
                    }
                    state->_onConnectionCallback(Client(std::move(wsResult.value())));
                });
            return;
        }

        return state->_onConnectionCallback(Client(std::move(tcpClient)));
    } else if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&state->_server.value())) {
        scaler::wrapper::uv::Pipe pipeClient = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Pipe::init(state->_loop, false));
        UV_EXIT_ON_ERROR(pipeServer->accept(pipeClient));
        return state->_onConnectionCallback(Client(std::move(pipeClient)));
    } else {
        std::unreachable();
    }
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler
