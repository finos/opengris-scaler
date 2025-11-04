#include "scaler/ymq/internal/raw_connection_tcp_fd.h"

#include <algorithm>
#include <cassert>  // assert

#include "scaler/error/error.h"
#include "scaler/ymq/internal/defs.h"

namespace scaler {
namespace ymq {

std::pair<uint64_t, RawConnectionTCPFD::IOStatus> RawConnectionTCPFD::tryReadUntilComplete(void* dest, size_t size)
{
    uint64_t cnt = 0;
    while (size) {
        const auto current = readBytes((char*)dest + cnt, size);
        if (current) {
            cnt += current.value();
            size -= current.value();
        } else {
            return {cnt, current.error()};
        }
    }
    return {cnt, IOStatus::MoreBytesAvailable};
}

std::pair<uint64_t, RawConnectionTCPFD::IOStatus> RawConnectionTCPFD::tryWriteUntilComplete(
    const std::vector<std::pair<void*, size_t>>& buffers)
{
    if (buffers.empty()) {
        return {0, IOStatus::MoreBytesAvailable};
    }

    std::vector<size_t> prefixSum(buffers.size() + 1);
    for (size_t i = 0; i < buffers.size(); ++i) {
        prefixSum[i + 1] = prefixSum[i] + buffers[i].second;
    }
    const size_t total = prefixSum.back();

    size_t sent = 0;
    while (sent != total) {
        auto unfinished = std::upper_bound(prefixSum.begin(), prefixSum.end(), sent);
        --unfinished;

        std::vector<std::pair<void*, size_t>> currentBuffers;

        auto begin          = buffers.begin() + std::distance(prefixSum.begin(), unfinished);
        const size_t remain = sent - *unfinished;

        currentBuffers.push_back({(char*)begin->first + remain, begin->second - remain});
        while (++begin != buffers.end()) {
            currentBuffers.push_back(*begin);
        }

        const auto res = writeBytes(currentBuffers);
        if (res) {
            sent += res.value();
        } else {
            return {sent, res.error()};
        }
    }
    return {total, IOStatus::MoreBytesAvailable};
}

std::expected<uint64_t, RawConnectionTCPFD::IOStatus> RawConnectionTCPFD::readBytes(void* dest, size_t size)
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
#ifdef __linux__
        // handle Linux errors
        const int myErrno = errno;
        if (myErrno == ECONNRESET) {
            if (_socketStatus == SocketStatus::Disconnecting || _socketStatus == SocketStatus::Disconnected) {
                _socketStatus = SocketStatus::Disconnected;
                return std::unexpected {IOStatus::Disconnected};
            } else {
                return std::unexpected {IOStatus::Aborted};
            }
        }
        if (myErrno == EAGAIN || myErrno == EWOULDBLOCK) {
            return std::unexpected {IOStatus::Drained};
        } else {
            const int myErrno = errno;
            switch (myErrno) {
                case EBADF:
                case EISDIR:
                case EINVAL:
                    unrecoverableError({
                        Error::ErrorCode::CoreBug,
                        "Originated from",
                        "recv(2)",
                        "Errno is",
                        strerror(myErrno),
                        "_fd",
                        _fd,
                        "dest",
                        (void*)dest,
                        "size",
                        size,
                    });

                case EINTR:
                    unrecoverableError({
                        Error::ErrorCode::SignalNotSupported,
                        "Originated from",
                        "recv(2)",
                        "Errno is",
                        strerror(myErrno),
                    });

                case EFAULT:
                case EIO:
                default:
                    unrecoverableError({
                        Error::ErrorCode::ConfigurationError,
                        "Originated from",
                        "recv(2)",
                        "Errno is",
                        strerror(myErrno),
                    });
            }
        }
#endif  // __linux__

#ifdef _WIN32
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
#endif  // _WIN32
    }

    std::unreachable();
}

std::expected<uint64_t, RawConnectionTCPFD::IOStatus> RawConnectionTCPFD::writeBytes(
    const std::vector<std::pair<void*, size_t>>& buffers)
{
    assert(buffers.size());
#ifdef _WIN32
#define iovec    ::WSABUF
#define IOV_MAX  (1024)
#define iov_base buf
#define iov_len  len
#endif  // _WIN32

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

#ifdef _WIN32
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
#endif  // _WIN32

#ifdef __linux__
    struct msghdr msg {};
    msg.msg_iov    = iovecs.data();
    msg.msg_iovlen = iovecs.size();

    ssize_t bytesSent = ::sendmsg(_fd, &msg, MSG_NOSIGNAL);
    if (bytesSent == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return std::unexpected {IOStatus::Drained};
        } else {
            const int myErrno = errno;
            switch (myErrno) {
                case EAFNOSUPPORT:
                case EBADF:
                case EINVAL:
                case EMSGSIZE:
                case ENOTCONN:
                case ENOTSOCK:
                case EOPNOTSUPP:
                case ENAMETOOLONG:
                case ENOENT:
                case ENOTDIR:
                case ELOOP:
                case EDESTADDRREQ:
                case EHOSTUNREACH:
                case EISCONN:
                    unrecoverableError({
                        Error::ErrorCode::CoreBug,
                        "Originated from",
                        "sendmsg(2)",
                        "Errno is",
                        strerror(myErrno),
                        "_fd",
                        _fd,
                        "msg.msg_iovlen",
                        msg.msg_iovlen,
                    });
                    break;

                case ECONNRESET:
                // We maybe need to handle this differently, conditionally return IOStatus::Disconnected
                case EPIPE: {
                    return std::unexpected {IOStatus::Aborted};
                    break;
                }

                case EINTR:
                    unrecoverableError({
                        Error::ErrorCode::SignalNotSupported,
                        "Originated from",
                        "sendmsg(2)",
                        "Errno is",
                        strerror(myErrno),
                    });
                    break;

                case EIO:
                case EACCES:
                case ENETDOWN:
                case ENETUNREACH:
                case ENOBUFS:
                case ENOMEM:
                default:
                    unrecoverableError({
                        Error::ErrorCode::ConfigurationError,
                        "Originated from",
                        "sendmsg(2)",
                        "Errno is",
                        strerror(myErrno),
                    });
                    break;
            }
        }
    }

    return bytesSent;
#endif  // __linux__
}

// TODO: This notifyHandle is a bad name but I don't have a better name for it now.
// Later, I will give it a better name. The purpose of it is to just "pass something"
// to the event loop.
bool RawConnectionTCPFD::prepareReadBytes(void* notifyHandle)
{
#ifdef _WIN32
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
#endif  // _WIN32

#ifdef __linux__
    return false;  // ???

#endif  // __linux__
}

// TODO: Think more about this notifyHandle, it used to be _eventManager.get()
std::pair<size_t, bool> RawConnectionTCPFD::prepareWriteBytes(void* dest, size_t size, void* notifyHandle)
{
    (void)size;
#ifdef _WIN32
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
#endif  // _WIN32

#ifdef __linux__
    (void)notifyHandle;
    return {0, false};

#endif  // __linux__
}

void RawConnectionTCPFD::shutdownRead() noexcept
{
#ifdef __linux__
    shutdown(_fd, SHUT_RD);
#endif  // __linux__
#ifdef _WIN32
    shutdown(_fd, SD_RECEIVE);
#endif  // _WIN32
}

void RawConnectionTCPFD::shutdownWrite() noexcept
{
#ifdef __linux__
    shutdown(_fd, SHUT_WR);
#endif  // __linux__
#ifdef _WIN32
    shutdown(_fd, SD_SEND);
#endif  // _WIN32
    _socketStatus = SocketStatus::Disconnecting;
}

void RawConnectionTCPFD::shutdownBoth() noexcept
{
    shutdownWrite();
    shutdownRead();
}

void RawConnectionTCPFD::closeAndZero() noexcept
{
#ifdef __linux__
    close(_fd);
#endif  // __linux__
#ifdef _WIN32
    closesocket(_fd);
#endif  // _WIN32
    _fd           = 0;
    _socketStatus = SocketStatus::Disconnected;
}

}  // namespace ymq
}  // namespace scaler
