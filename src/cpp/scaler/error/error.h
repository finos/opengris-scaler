#pragma once

#include <algorithm>
#include <exception>  // std::terminate
#include <format>
#include <functional>
#include <iostream>
#include <string>

#include "scaler/ymq/timestamp.h"
#include "scaler/ymq/utils.h"

namespace scaler {
namespace ymq {

struct Error: public std::exception {
    enum struct ErrorCode {
        Uninit,
        InvalidPortFormat,
        InvalidAddressFormat,
        RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery,
        ConnectorSocketClosedByRemoteEnd,
        SocketStopRequested,
        SysCallError,
    };

    // NOTE:
    // Format:
    //    [Timestamp, ": ", Error Explanation, ": ", Other]
    // For user calls errors:
    //     Other := ["Originated from", Function Name, items...]
    // For system calls errors:
    //     Other := ["Originated from", Function Name, "Errno is", strerror(errno), items...]
    template <typename... Args>
    constexpr Error(ErrorCode e, Args&&... args) noexcept
        : _errorCode(e), _logMsg(argsToString(Timestamp {}, convertErrorToExplanation(e), std::forward<Args>(args)...))
    {
    }

    Error() noexcept: _errorCode(ErrorCode::Uninit) {}

    static inline constexpr std::string_view convertErrorToExplanation(ErrorCode e) noexcept
    {
        switch (e) {
            case ErrorCode::Uninit: return "";
            case ErrorCode::InvalidPortFormat: return "Invalid port format, example input \"tcp://127.0.0.1:2345\"";
            case ErrorCode::InvalidAddressFormat:
                return "Invalid address format, example input \"tcp://127.0.0.1:2345\" or "
                       "\"ipc:///tmp/domain_socket_name.sock\"";
            case ErrorCode::RemoteEndDisconnectedOnSocketWithoutGuaranteedDelivery:
                return "You are using Unicast or Multicast sockets, which do not support guaranteed message delivery, "
                       "and the connection(s) disconnects";
            case ErrorCode::ConnectorSocketClosedByRemoteEnd:
                return "Your connector socket connection is closed by remote end";
            case ErrorCode::SocketStopRequested: return "Current socket is requested to stop by another thread";
            case ErrorCode::SysCallError: return "A system call error occurred";
        }
        std::cerr << "Unrecognized ErrorCode value, program exits\n";
        std::exit(1);
    }

    constexpr const char* what() const noexcept override { return _logMsg.c_str(); }

    ErrorCode _errorCode;
    std::string _logMsg;
};

}  // namespace ymq
}  // namespace scaler

template <>
struct std::formatter<scaler::ymq::Error, char> {
    template <class ParseContext>
    constexpr ParseContext::iterator parse(ParseContext& ctx) noexcept
    {
        return ctx.begin();
    }

    template <class FmtContext>
    constexpr FmtContext::iterator format(scaler::ymq::Error e, FmtContext& ctx) const noexcept
    {
        return std::ranges::copy(e._logMsg, ctx.out()).out;
    }
};

using UnrecoverableErrorFunctionHookPtr = std::function<void(scaler::ymq::Error)>;

[[noreturn]] inline void defaultUnrecoverableError(scaler::ymq::Error e) noexcept
{
    std::cerr << e.what() << '\n';
    std::exit(1);
}

inline UnrecoverableErrorFunctionHookPtr unrecoverableErrorFunctionHookPtr = defaultUnrecoverableError;

[[noreturn]] inline void unrecoverableError(scaler::ymq::Error e) noexcept
{
    unrecoverableErrorFunctionHookPtr(std::move(e));
    std::exit(1);
}
