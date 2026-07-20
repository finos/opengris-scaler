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

namespace details {

Error toYMQError(scaler::wrapper::uv::Error uvError) noexcept
{
    return Error {
        Error::ErrorCode::SysCallError,
        "Originated from",
        "AcceptServer::init",
        "Error code",
        uvError.name(),
        uvError.message(),
    };
}

}  // namespace details

std::expected<AcceptServer, scaler::ymq::Error> AcceptServer::init(
    scaler::wrapper::uv::Loop& loop, Address address, ConnectionCallback onConnectionCallback) noexcept
{
    auto sslContext = address.getSSLContext();
    if (!sslContext.has_value()) {
        return std::unexpected {std::move(sslContext.error())};
    }

    std::optional<Server> server;

    switch (address.type()) {
        case Address::Type::TCP: {
            if (address.secure()) {
                auto secureServer = scaler::wrapper::openssl::SecureServer::init(loop);
                if (!secureServer.has_value()) {
                    return std::unexpected {details::toYMQError(secureServer.error())};
                }
                if (auto bindResult = secureServer->bind(address.asTCP(), uv_tcp_flags(0)); !bindResult.has_value()) {
                    return std::unexpected {details::toYMQError(bindResult.error())};
                }
                server = std::move(secureServer.value());
            } else {
                auto tcpServer = scaler::wrapper::uv::TCPServer::init(loop);
                if (!tcpServer.has_value()) {
                    return std::unexpected {details::toYMQError(tcpServer.error())};
                }
                if (auto bindResult = tcpServer->bind(address.asTCP(), uv_tcp_flags(0)); !bindResult.has_value()) {
                    return std::unexpected {details::toYMQError(bindResult.error())};
                }
                server = std::move(tcpServer.value());
            }
            break;
        }
        case Address::Type::IPC: {
            auto pipeServer = scaler::wrapper::uv::PipeServer::init(loop, false);
            if (!pipeServer.has_value()) {
                return std::unexpected {details::toYMQError(pipeServer.error())};
            }
            if (auto bindResult = pipeServer->bind(address.asIPC()); !bindResult.has_value()) {
                return std::unexpected {details::toYMQError(bindResult.error())};
            }
            server = std::move(pipeServer.value());
            break;
        }
        case Address::Type::WebSocket: {
            auto wsServer = WebSocketServer::init(loop, address.secure());

            if (!wsServer.has_value()) {
                return std::unexpected {details::toYMQError(wsServer.error())};
            }
            if (auto bindResult = wsServer->bind(address.asWebSocket(), uv_tcp_flags(0)); !bindResult.has_value()) {
                return std::unexpected {details::toYMQError(bindResult.error())};
            }
            server = std::move(wsServer.value());
            break;
        }
        default: std::unreachable();
    }

    auto state = std::make_shared<State>(
        loop, std::move(onConnectionCallback), std::move(server.value()), std::move(sslContext.value()));

    auto listenCallback = std::bind_front(&AcceptServer::onConnection, state);

    // On Linux, a bind() EADDRINUSE is delayed by libuv and only reported here, at listen().
    std::expected<void, scaler::wrapper::uv::Error> listenResult;
    if (auto* tcpServer = std::get_if<scaler::wrapper::uv::TCPServer>(&state->_server.value())) {
        listenResult = tcpServer->listen(serverListenBacklog, std::move(listenCallback));
    } else if (auto* secureServer = std::get_if<scaler::wrapper::openssl::SecureServer>(&state->_server.value())) {
        listenResult = secureServer->listen(serverListenBacklog, std::move(listenCallback));
    } else if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&state->_server.value())) {
        listenResult = pipeServer->listen(serverListenBacklog, std::move(listenCallback));
    } else if (auto* wsServer = std::get_if<WebSocketServer>(&state->_server.value())) {
        listenResult = wsServer->listen(serverListenBacklog, std::move(listenCallback));
    } else {
        std::unreachable();
    }

    if (!listenResult.has_value()) {
        return std::unexpected {details::toYMQError(listenResult.error())};
    }

    return AcceptServer {std::move(state)};
}

AcceptServer::AcceptServer(std::shared_ptr<State> state) noexcept: _state(std::move(state))
{
}

AcceptServer::State::State(
    scaler::wrapper::uv::Loop& loop,
    ConnectionCallback onConnectionCallback,
    Server server,
    std::optional<scaler::wrapper::openssl::SSLContext> sslContext) noexcept
    : _loop(loop)
    , _onConnectionCallback(std::move(onConnectionCallback))
    , _server(std::move(server))
    , _sslContext(std::move(sslContext))
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
        return Address {UV_EXIT_ON_ERROR(tcpServer->getSockName())};
    } else if (auto* secureServer = std::get_if<scaler::wrapper::openssl::SecureServer>(&_state->_server.value())) {
        return Address {UV_EXIT_ON_ERROR(secureServer->getSockName()), true};
    } else if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&_state->_server.value())) {
        return Address {UV_EXIT_ON_ERROR(pipeServer->getSockName())};
    } else if (auto* wsServer = std::get_if<WebSocketServer>(&_state->_server.value())) {
        return Address {UV_EXIT_ON_ERROR(wsServer->getSockName()), _state->_sslContext.has_value()};
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
        return state->_onConnectionCallback(Client(std::move(tcpClient)));
    } else if (auto* secureServer = std::get_if<scaler::wrapper::openssl::SecureServer>(&state->_server.value())) {
        scaler::wrapper::openssl::SecureSocket secureClient =
            UV_EXIT_ON_ERROR(scaler::wrapper::openssl::SecureSocket::init(state->_loop, state->_sslContext.value()));
        UV_EXIT_ON_ERROR(secureServer->accept(secureClient));
        return state->_onConnectionCallback(Client(std::move(secureClient)));
    } else if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&state->_server.value())) {
        scaler::wrapper::uv::Pipe pipeClient = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Pipe::init(state->_loop, false));
        UV_EXIT_ON_ERROR(pipeServer->accept(pipeClient));
        return state->_onConnectionCallback(Client(std::move(pipeClient)));
    } else if (auto* wsServer = std::get_if<WebSocketServer>(&state->_server.value())) {
        // Hold the socket in a unique_ptr so it stays alive until the handshake completes.
        auto wsClient = std::make_unique<WebSocketStream>(
            UV_EXIT_ON_ERROR(WebSocketStream::init(state->_loop, state->_sslContext)));

        WebSocketStream& wsClientRef = *wsClient;
        UV_EXIT_ON_ERROR(wsServer->accept(
            wsClientRef,
            [state, wsClient = std::move(wsClient)](std::expected<void, scaler::wrapper::uv::Error> result) mutable {
                if (!result.has_value()) {
                    return;
                }
                state->_onConnectionCallback(Client(std::move(*wsClient)));
            }));
        return;
    } else {
        std::unreachable();
    }
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler
