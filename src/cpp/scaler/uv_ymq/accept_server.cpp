#include "scaler/uv_ymq/accept_server.h"

#include <uv.h>

#include <cassert>
#include <functional>
#include <utility>

#include "scaler/uv_ymq/configuration.h"
#include "scaler/wrapper/uv/pipe.h"
#include "scaler/wrapper/uv/socket_address.h"
#include "scaler/wrapper/uv/tcp.h"

namespace scaler {
namespace uv_ymq {

AcceptServer::AcceptServer(
    scaler::wrapper::uv::Loop& loop, Address address, ConnectionCallback onConnectionCallback) noexcept
{
    std::optional<Server> server;

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
        default: std::unreachable();
    }

    _state = std::make_shared<State>(loop, std::move(onConnectionCallback), std::move(server.value()));

    if (auto* tcpServer = std::get_if<scaler::wrapper::uv::TCPServer>(&_state->_server.value())) {
        UV_EXIT_ON_ERROR(tcpServer->listen(serverListenBacklog, std::bind_front(&AcceptServer::onConnection, _state)));
    } else if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&_state->_server.value())) {
        UV_EXIT_ON_ERROR(pipeServer->listen(serverListenBacklog, std::bind_front(&AcceptServer::onConnection, _state)));
    } else {
        std::unreachable();
    }
}

AcceptServer::State::State(
    scaler::wrapper::uv::Loop& loop, ConnectionCallback onConnectionCallback, Server server) noexcept
    : _loop(loop), _onConnectionCallback(std::move(onConnectionCallback)), _server(std::move(server))
{
}

AcceptServer::~AcceptServer() noexcept
{
    disconnect();
}

Address AcceptServer::address() const noexcept
{
    if (auto* tcpServer = std::get_if<scaler::wrapper::uv::TCPServer>(&_state->_server.value())) {
        return Address {UV_EXIT_ON_ERROR(tcpServer->getSockName())};
    } else if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&_state->_server.value())) {
        return Address {UV_EXIT_ON_ERROR(pipeServer->getSockName())};
    } else {
        std::unreachable();
    }
}

void AcceptServer::disconnect() noexcept
{
    _state->_server = std::nullopt;
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
        return state->_onConnectionCallback(std::move(tcpClient));
    } else if (auto* pipeServer = std::get_if<scaler::wrapper::uv::PipeServer>(&state->_server.value())) {
        scaler::wrapper::uv::Pipe pipeClient = UV_EXIT_ON_ERROR(scaler::wrapper::uv::Pipe::init(state->_loop, false));
        UV_EXIT_ON_ERROR(pipeServer->accept(pipeClient));
        return state->_onConnectionCallback(std::move(pipeClient));
    } else {
        std::unreachable();
    }
}

}  // namespace uv_ymq
}  // namespace scaler
