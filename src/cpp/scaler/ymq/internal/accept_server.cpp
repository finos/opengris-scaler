#include "scaler/ymq/internal/accept_server.h"

#include <uv.h>

#include <cassert>
#include <filesystem>
#include <functional>
#include <utility>

#include "scaler/wrapper/uv/pipe.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/internal/websocket_stream.h"

namespace scaler {
namespace ymq {
namespace internal {

std::expected<AcceptServer, scaler::wrapper::uv::Error> AcceptServer::init(
    scaler::wrapper::uv::Loop& loop, Address address, ConnectionCallback onConnectionCallback) noexcept
{
    std::optional<Server> server;
    std::optional<WebSocketAddress> webSocketAddress;

    switch (address.type()) {
        case Address::Type::TCP: {
            auto tcpServer = scaler::wrapper::uv::TCPServer::init(loop);
            if (!tcpServer.has_value()) {
                return std::unexpected {tcpServer.error()};
            }
            if (auto bindResult = tcpServer->bind(address.asTCP(), uv_tcp_flags(0)); !bindResult.has_value()) {
                return std::unexpected {bindResult.error()};
            }
            server = std::move(tcpServer.value());
            break;
        }
        case Address::Type::IPC: {
            auto pipeServer = scaler::wrapper::uv::PipeServer::init(loop, false);
            if (!pipeServer.has_value()) {
                return std::unexpected {pipeServer.error()};
            }
            if (auto bindResult = pipeServer->bind(address.asIPC()); !bindResult.has_value()) {
                return std::unexpected {bindResult.error()};
            }
            server = std::move(pipeServer.value());
            break;
        }
        case Address::Type::WebSocket: {
            // WebSocket runs over TCP; bind a TCPServer to the resolved TCP address.
            webSocketAddress = address.asWebSocket();
            auto tcpServer   = scaler::wrapper::uv::TCPServer::init(loop);
            if (!tcpServer.has_value()) {
                return std::unexpected {tcpServer.error()};
            }
            if (auto bindResult = tcpServer->bind(webSocketAddress->tcpAddress, uv_tcp_flags(0));
                !bindResult.has_value()) {
                return std::unexpected {bindResult.error()};
            }
            server = std::move(tcpServer.value());
            break;
        }
        default: std::unreachable();
    }

    AcceptServer acceptServer;
    acceptServer._state = std::make_shared<State>(
        loop, std::move(onConnectionCallback), std::move(server.value()), std::move(webSocketAddress));

    // On Linux, a bind() EADDRINUSE is delayed by libuv and only reported here, at listen().
    std::expected<void, scaler::wrapper::uv::Error> listenResult;
    if (auto* tcpServer = std::get_if<scaler::wrapper::uv::TCPServer>(&acceptServer._state->_server.value())) {
        listenResult =
            tcpServer->listen(serverListenBacklog, std::bind_front(&AcceptServer::onConnection, acceptServer._state));
    } else if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&acceptServer._state->_server.value())) {
        listenResult =
            pipeServer->listen(serverListenBacklog, std::bind_front(&AcceptServer::onConnection, acceptServer._state));
    } else {
        std::unreachable();
    }

    if (!listenResult.has_value()) {
        return std::unexpected {listenResult.error()};
    }

    return acceptServer;
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
    UV_EXIT_ON_ERROR(result);

    if (state->_server == std::nullopt) {
        return;  // server disconnecting
    }

    if (auto* tcpServer = std::get_if<scaler::wrapper::uv::TCPServer>(&state->_server.value())) {
        scaler::wrapper::uv::TCPSocket tcpClient = UV_EXIT_ON_ERROR(scaler::wrapper::uv::TCPSocket::init(state->_loop));
        UV_EXIT_ON_ERROR(tcpServer->accept(tcpClient));

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
