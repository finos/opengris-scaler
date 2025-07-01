#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <string.h>

#include <cassert>
#include <expected>
#include <iostream>

inline std::expected<sockaddr, int> stringToSockaddr(const std::string& address) {
    // Check and strip the "tcp://" prefix
    const std::string prefix = "tcp://";
    if (address.substr(0, prefix.size()) != prefix) {
        exit(-1);
        // unrecoverableError({Error::InvalidAddressFormat}, "Your input is:", address);
    }

    std::string addr_part = address.substr(prefix.size());
    size_t colon_pos      = addr_part.find(':');
    if (colon_pos == std::string::npos) {
        exit(-1);
        // unrecoverableError({Error::InvalidAddressFormat}, "Your input is:", address);
    }

    std::string ip       = addr_part.substr(0, colon_pos);
    std::string port_str = addr_part.substr(colon_pos + 1);

    int port = 0;
    try {
        port = std::stoi(port_str);
    } catch (...) {
        exit(-1);
        // unrecoverableError({Error::InvalidAddressFormat}, "Your input is:", address);
    }

    sockaddr_in out_addr {};
    out_addr.sin_family = AF_INET;
    out_addr.sin_port   = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &out_addr.sin_addr) <= 0) {
        exit(-1);
        // unrecoverableError({Error::InvalidAddressFormat}, "Your input is:", address);
    }

    return *(sockaddr*)&out_addr;
}

inline int setNoDelay(int fd) {
    int optval = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) == -1) {
        exit(-1);
        // unrecoverableError({errno}, "From", "setsockopt", "TCP_NODELAY cannot be set.");
    }

    return fd;
}

inline sockaddr getLocalAddr(int fd) {
    sockaddr localAddr     = {};
    socklen_t localAddrLen = sizeof(localAddr);
    if (getsockname(fd, &localAddr, &localAddrLen) == -1) {
        perror("getsockname");
        fprintf(stderr, "Cannot get local address\n");
    }
    return localAddr;
}

inline sockaddr getRemoteAddr(int fd) {
    sockaddr remoteAddr     = {};
    socklen_t remoteAddrLen = sizeof(remoteAddr);
    if (getpeername(fd, &remoteAddr, &remoteAddrLen) == -1) {
        perror("getpeername");
        fprintf(stderr, "Cannot get remote address\n");
    }
    return remoteAddr;
}
