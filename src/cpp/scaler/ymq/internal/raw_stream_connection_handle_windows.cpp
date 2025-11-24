#ifdef _WIN32
#include <algorithm>
#include <cassert>  // assert

#include "scaler/error/error.h"
#include "scaler/ymq/internal/defs.h"
#include "scaler/ymq/internal/network_utils.h"
#include "scaler/ymq/internal/raw_stream_connection_handle.h"

namespace scaler {
namespace ymq {

std::pair<uint64_t, RawStreamConnectionHandle::IOStatus> RawStreamConnectionHandle::tryReadUntilComplete(
    void* dest, size_t size)
{
    return scaler::ymq::tryReadUntilComplete(
        dest, size, [&](char* dest, size_t size) { return this->readBytes(dest, size); });
}

std::pair<uint64_t, RawStreamConnectionHandle::IOStatus> RawStreamConnectionHandle::tryWriteUntilComplete(
    const std::vector<std::pair<void*, size_t>>& buffers)
{
    return scaler::ymq::tryWriteUntilComplete(buffers, [&](std::vector<std::pair<void*, size_t>> currentBuffers) {
        return this->writeBytes(currentBuffers);
    });
}

void RawStreamConnectionHandle::shutdownBoth() noexcept
{
    shutdownWrite();
    shutdownRead();
}

std::expected<uint64_t, RawStreamConnectionHandle::IOStatus> RawStreamConnectionHandle::readBytes(
    void* dest, size_t size)
{
    assert(_fd);
    assert(dest);
    assert(size);

    constexpr const int flags = 0;

    const int n = ::recv(_fd, (char*)dest, size, flags);
    if (n > 0) {
        return n;
    }

    if (!n) {
        _socketStatus = SocketStatus::Disconnected;
        return std::unexpected {IOStatus::Disconnected};
    }

    if (n == -1) {
        const int myErrno = WSAGetLastError();
        if (myErrno == WSAEWOULDBLOCK) {
            return std::unexpected {IOStatus::Drained};
        }
        if (myErrno == WSAECONNRESET || myErrno == WSAENOTSOCK || myErrno == WSAECONNABORTED) {
            if (_socketStatus == SocketStatus::Disconnecting || _socketStatus == SocketStatus::Disconnected) {
                _socketStatus = SocketStatus::Disconnected;
                return std::unexpected {IOStatus::Disconnected};
            } else {
                return std::unexpected {IOStatus::Aborted};
            }
        } else {
            // NOTE: On Windows we don't have signals and weird IO Errors
            unrecoverableError({
                Error::ErrorCode::CoreBug,
                "Originated from",
                "recv(2)",
                "Errno is",
                myErrno,
                "_fd",
                _fd,
                "dest",
                (void*)dest,
                "size",
                size,
            });
        }
    }

    std::unreachable();
}

std::expected<uint64_t, RawStreamConnectionHandle::IOStatus> RawStreamConnectionHandle::writeBytes(
    const std::vector<std::pair<void*, size_t>>& buffers)
{
    assert(buffers.size());
#define iovec    ::WSABUF
#define IOV_MAX  (1024)
#define iov_base buf
#define iov_len  len

    std::vector<iovec> iovecs;
    iovecs.reserve(IOV_MAX);

    size_t total = 0;
    for (const auto& [ptr, len]: buffers) {
        if (iovecs.size() == IOV_MAX) {
            break;
        }
        iovec current;
        current.iov_base = (char*)ptr;
        current.iov_len  = len;
        iovecs.push_back(std::move(current));
        total += current.iov_len;
        // iovecs.emplace_back((void*)ptr, len);
    }

    assert(total);
    (void)total;

    if (iovecs.empty()) {
        return 0;
    }

    DWORD bytesSent {};
    const int sendToResult = WSASendTo(_fd, iovecs.data(), iovecs.size(), &bytesSent, 0, nullptr, 0, nullptr, nullptr);
    if (sendToResult == 0) {
        return bytesSent;
    }
    const int myErrno = WSAGetLastError();
    if (myErrno == WSAEWOULDBLOCK) {
        return std::unexpected {IOStatus::Drained};
    }
    if (myErrno == WSAESHUTDOWN || myErrno == WSAENOTCONN || myErrno == WSAECONNRESET) {
        if (_socketStatus == SocketStatus::Disconnecting || _socketStatus == SocketStatus::Disconnected) {
            _socketStatus = SocketStatus::Disconnected;
            return std::unexpected {IOStatus::Disconnected};
        } else {
            return std::unexpected {IOStatus::Aborted};
        }
    }
    unrecoverableError({
        Error::ErrorCode::CoreBug,
        "Originated from",
        "WSASendTo",
        "Errno is",
        myErrno,
        "_fd",
        _fd,
        "iovecs.size()",
        iovecs.size(),
    });
#undef iovec
#undef IOV_MAX
#undef iov_base
#undef iov_len
}

// TODO: This notifyHandle is a bad name but I don't have a better name for it now.
// Later, I will give it a better name. The purpose of it is to just "pass something"
// to the event loop.
bool RawStreamConnectionHandle::prepareReadBytes(void* notifyHandle)
{
    // TODO: This need rewrite to better logic
    if (!_fd) {
        return false;
    }
    const bool ok = ReadFile((HANDLE)(SOCKET)_fd, nullptr, 0, nullptr, (LPOVERLAPPED)notifyHandle);
    if (ok) {
        // onRead();
        return true;
    }
    const auto lastError = GetLastError();
    if (lastError == ERROR_IO_PENDING) {
        return false;
    }
    unrecoverableError({
        Error::ErrorCode::CoreBug,
        "Originated from",
        "ReadFile",
        "Errno is",
        lastError,
        "_fd",
        _fd,
    });
    std::unreachable();
}

// TODO: Think more about this notifyHandle, it used to be _eventManager.get()
std::pair<size_t, bool> RawStreamConnectionHandle::prepareWriteBytes(void* dest, size_t size, void* notifyHandle)
{
    (void)size;
    // NOTE: Precondition is the queue still has messages (perhaps a partial one).
    const bool writeFileRes = WriteFile((HANDLE)(SOCKET)_fd, dest, 1, nullptr, (LPOVERLAPPED)notifyHandle);
    if (writeFileRes) {
        return {1, true};
    }

    const auto lastError = GetLastError();
    if (lastError == ERROR_IO_PENDING) {
        return {1, false};
    }
    unrecoverableError({
        Error::ErrorCode::CoreBug,
        "Originated from",
        "prepareWriteBytes",
        "Errno is",
        lastError,
    });
}

void RawStreamConnectionHandle::shutdownRead() noexcept
{
    shutdown(_fd, SD_RECEIVE);
}

void RawStreamConnectionHandle::shutdownWrite() noexcept
{
    shutdown(_fd, SD_SEND);
    _socketStatus = SocketStatus::Disconnecting;
}

void RawStreamConnectionHandle::closeAndZero() noexcept
{
    closesocket(_fd);
    _fd           = 0;
    _socketStatus = SocketStatus::Disconnected;
}

}  // namespace ymq
}  // namespace scaler
#endif
