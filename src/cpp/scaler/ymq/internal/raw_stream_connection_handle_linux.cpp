#ifdef __linux__
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
    }

    std::unreachable();
}

std::expected<uint64_t, RawStreamConnectionHandle::IOStatus> RawStreamConnectionHandle::writeBytes(
    const std::vector<std::pair<void*, size_t>>& buffers)
{
    assert(buffers.size());

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
    }

    assert(total);
    (void)total;

    if (iovecs.empty()) {
        return 0;
    }

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
}

bool RawStreamConnectionHandle::prepareReadBytes(void* notifyHandle)
{
    (void)notifyHandle;
    return false;
}

std::pair<size_t, bool> RawStreamConnectionHandle::prepareWriteBytes(void* dest, size_t size, void* notifyHandle)
{
    (void)size;
    (void)notifyHandle;
    return {0, false};
}

void RawStreamConnectionHandle::shutdownRead() noexcept
{
    shutdown(_fd, SHUT_RD);
}

void RawStreamConnectionHandle::shutdownWrite() noexcept
{
    shutdown(_fd, SHUT_WR);
    _socketStatus = SocketStatus::Disconnecting;
}

void RawStreamConnectionHandle::closeAndZero() noexcept
{
    close(_fd);
    _fd           = 0;
    _socketStatus = SocketStatus::Disconnected;
}

}  // namespace ymq
}  // namespace scaler
#endif
