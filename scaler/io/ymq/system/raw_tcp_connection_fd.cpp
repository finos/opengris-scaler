#include "scaler/io/ymq/system/raw_tcp_connection_fd.h"

#include <cassert>
#include <cstdint>
#include <utility>

#include "scaler/io/ymq/error.h"
#ifdef __linux__
#include <errno.h>
#include <limits.h>
#include <sys/socket.h>
#include <unistd.h>
#endif  // __linux__
#ifdef _WIN32
// clang-format off
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
// clang-format on
#endif  // _WIN32

namespace scaler {
namespace ymq {

std::expected<uint64_t, RawTCPConnectionFD::IOError> RawTCPConnectionFD::readBytes(char* dest, size_t len)
{
    assert(_fd);
    assert(len);
    assert(dest);

    constexpr const int flags = 0;

    const int n = ::recv(_fd, dest, len, flags);
    if (n > 0) {
        return n;
    }

    if (!n) {
        return std::unexpected {IOError::Disconnected};
    }

    if (n == -1) {
#ifdef __linux__
        // handle Linux errors
        const int myErrno = errno;
        if (myErrno == ECONNRESET) {
            return std::unexpected {IOError::Aborted};
        }
        if (myErrno == EAGAIN || myErrno == EWOULDBLOCK) {
            return std::unexpected {IOError::Drained};
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
                        "len",
                        len,
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
            return std::unexpected {IOError::Drained};
        }
        if (myErrno == WSAECONNRESET || myErrno == WSAENOTSOCK) {
            return std::unexpected {IOError::Aborted};
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
                "len",
                len,
            });
        }
#endif  // _WIN32
    }

    std::unreachable();
}

std::expected<uint64_t, RawTCPConnectionFD::IOError> RawTCPConnectionFD::writeBytes(
    std::vector<std::pair<const unsigned char*, size_t>> buffers)
{
#ifdef _WIN32
#define iovec    ::WSABUF
#define IOV_MAX  (1024)
#define iov_base buf
#define iov_len  len
#endif  // _WIN32

    std::vector<iovec> iovecs;
    iovecs.reserve(IOV_MAX);

    for (const auto& [ptr, len]: buffers) {
        if (iovecs.size() == IOV_MAX) {
            break;
        }
        iovec current;
        current.iov_base = (void*)ptr;
        current.iov_len  = len;
        iovecs.push_back(std::move(current));
        // iovecs.emplace_back((void*)ptr, len);
    }

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
        return std::unexpected {IOError::Drained};
    }
    if (myErrno == WSAESHUTDOWN || myErrno == WSAENOTCONN) {
        return std::unexpected {IOError::Aborted};
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
            return std::unexpected {IOError::Drained};
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
                case EPIPE: return std::unexpected {IOError::Aborted}; break;

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

bool RawTCPConnectionFD::prepareReadBytes(void* notifyHandle)
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
std::pair<size_t, bool> RawTCPConnectionFD::prepareWriteBytes(char* dest, size_t len, void* notifyHandle)
{
    (void)len;
#ifdef _WIN32
    // NOTE: Precondition is the queue still has messages (perhaps a partial one).
    // We don't need to update the queue because trySendQueuedMessages is okay with a complete message in front.

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
    return {0, false};

#endif  // __linux__
}

}  // namespace ymq
}  // namespace scaler
