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
#undef max
#endif  // _WIN32
