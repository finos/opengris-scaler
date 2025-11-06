#pragma once

// Headers
#ifdef __linux__
#include <arpa/inet.h>  // inet_pton
#include <errno.h>      // EAGAIN etc.
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>  // ::recv
#include <sys/socket.h>
#include <sys/time.h>  // itimerspec
#include <sys/timerfd.h>
#include <unistd.h>
#endif  // __linux__
#ifdef _WIN32
// clang-format off
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <mswsock.h>
#include <ws2tcpip.h> // inet_pton
// clang-format on
#endif  // _WIN32

// Windows being evil
#ifdef _WIN32
#undef SendMessageCallback
#define __PRETTY_FUNCTION__ __FUNCSIG__
#define EPOLLIN             (0)
#define EPOLLOUT            (0)
#define EPOLLET             (0)
#endif  // _WIN32

namespace scaler {
namespace ymq {
inline auto GetErrorCode()
{
#ifdef __linux__
    return errno;
#endif  // __linux__
#ifdef _WIN32
    return WSAGetLastError();
#endif  // _WIN32
}

inline constexpr void CloseAndZeroSocket(auto& fd)
{
#ifdef __linux__
    close(fd);
#endif  // __linux__
#ifdef _WIN32
    closesocket(fd);
#endif  // _WIN32
    fd = 0;
}

#ifdef __linux__
using RawSocketType = int;
#endif  // __linux__
#ifdef _WIN32
using RawSocketType = SOCKET;
#endif  // _WIN32
}  // namespace ymq
}  // namespace scaler
