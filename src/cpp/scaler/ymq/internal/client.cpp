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

bool Client::isTCP() const noexcept
{
    return std::holds_alternative<scaler::wrapper::uv::TCPSocket>(_socket);
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
    } else {
        std::unreachable();
    }
}

std::expected<void, scaler::wrapper::uv::Error> Client::setNoDelay(bool enable) noexcept
{
    if (auto* tcp = std::get_if<scaler::wrapper::uv::TCPSocket>(&_socket)) {
        return tcp->nodelay(enable);
    }

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
    } else {
        std::unreachable();
    }

    return {};
}

std::expected<void, scaler::wrapper::uv::Error> Client::closeReset() noexcept
{
    auto* tcp = std::get_if<scaler::wrapper::uv::TCPSocket>(&_socket);
    assert(tcp && "closeReset() is only supported for TCP sockets");

    return tcp->closeReset();
}

}  // namespace internal
}  // namespace ymq
}  // namespace scaler
