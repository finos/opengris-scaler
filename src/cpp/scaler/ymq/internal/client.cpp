#include "scaler/ymq/internal/client.h"

#include <cassert>
#include <variant>

namespace scaler {
namespace ymq {
namespace internal {

Client::Client(scaler::wrapper::uv::TCPSocket socket) noexcept: _socket(std::move(socket))
{
}

Client::Client(scaler::wrapper::uv::Pipe pipe) noexcept: _socket(std::move(pipe))
{
}

Client::Client(WebSocketStream stream) noexcept: _socket(std::move(stream))
{
}

bool Client::isTCP() const noexcept
{
    return std::holds_alternative<scaler::wrapper::uv::TCPSocket>(_socket);
}

bool Client::isWebSocket() const noexcept
{
    return std::holds_alternative<WebSocketStream>(_socket);
}

std::expected<void, scaler::wrapper::uv::Error> Client::write(
    std::span<const std::span<const uint8_t>> buffers, scaler::wrapper::uv::WriteCallback callback) noexcept
{
    if (auto* tcp = std::get_if<scaler::wrapper::uv::TCPSocket>(&_socket)) {
        if (auto result = tcp->write(buffers, std::move(callback)); !result) {
            return std::unexpected(result.error());
        }
    } else if (auto* pipe = std::get_if<scaler::wrapper::uv::Pipe>(&_socket)) {
        if (auto result = pipe->write(buffers, std::move(callback)); !result) {
            return std::unexpected(result.error());
        }
    } else if (auto* ws = std::get_if<WebSocketStream>(&_socket)) {
        if (auto result = ws->write(buffers, std::move(callback)); !result) {
            return std::unexpected(result.error());
        }
    } else {
        std::unreachable();
    }

    return {};
}

std::expected<void, scaler::wrapper::uv::Error> Client::readStart(scaler::wrapper::uv::ReadCallback callback) noexcept
{
    if (auto* tcp = std::get_if<scaler::wrapper::uv::TCPSocket>(&_socket)) {
        return tcp->readStart(std::move(callback));
    } else if (auto* pipe = std::get_if<scaler::wrapper::uv::Pipe>(&_socket)) {
        return pipe->readStart(std::move(callback));
    } else if (auto* ws = std::get_if<WebSocketStream>(&_socket)) {
        return ws->readStart(std::move(callback));
    } else {
        std::unreachable();
    }
}

void Client::readStop() noexcept
{
    if (auto* tcp = std::get_if<scaler::wrapper::uv::TCPSocket>(&_socket)) {
        tcp->readStop();
    } else if (auto* pipe = std::get_if<scaler::wrapper::uv::Pipe>(&_socket)) {
        pipe->readStop();
    } else if (auto* ws = std::get_if<WebSocketStream>(&_socket)) {
        ws->readStop();
    } else {
        std::unreachable();
    }
}

std::expected<void, scaler::wrapper::uv::Error> Client::setNoDelay(bool enable) noexcept
{
    if (auto* tcp = std::get_if<scaler::wrapper::uv::TCPSocket>(&_socket)) {
        return tcp->nodelay(enable);
    }
    // WebSocket is TCP-backed but TCP_NODELAY is already set during connection setup.
    // IPC does not support TCP_NODELAY.
    return {};
}

std::expected<void, scaler::wrapper::uv::Error> Client::shutdown(
    scaler::wrapper::uv::ShutdownCallback callback) noexcept
{
    if (auto* tcp = std::get_if<scaler::wrapper::uv::TCPSocket>(&_socket)) {
        if (auto result = tcp->shutdown(std::move(callback)); !result) {
            return std::unexpected(result.error());
        }
    } else if (auto* pipe = std::get_if<scaler::wrapper::uv::Pipe>(&_socket)) {
        if (auto result = pipe->shutdown(std::move(callback)); !result) {
            return std::unexpected(result.error());
        }
    } else if (auto* ws = std::get_if<WebSocketStream>(&_socket)) {
        if (auto result = ws->shutdown(std::move(callback)); !result) {
            return std::unexpected(result.error());
        }
    } else {
        std::unreachable();
    }

    return {};
}

std::expected<void, scaler::wrapper::uv::Error> Client::closeReset() noexcept
{
    if (auto* tcp = std::get_if<scaler::wrapper::uv::TCPSocket>(&_socket)) {
        return tcp->closeReset();
    }
    if (auto* ws = std::get_if<WebSocketStream>(&_socket)) {
        return ws->closeReset();
    }
    assert(false && "closeReset() is only supported for TCP-based sockets");
    std::unreachable();
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler
