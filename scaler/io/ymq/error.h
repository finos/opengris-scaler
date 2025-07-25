#pragma once

#include <algorithm>
#include <exception>  // std::terminate
#include <format>
#include <functional>
#include <print>
#include <string>

#include "scaler/io/ymq/timestamp.h"
#include "scaler/io/ymq/utils.h"

namespace scaler {
namespace ymq {

struct Error: std::exception {
    enum struct ErrorCode {
        Uninit,
        InvalidPortFormat,
        InvalidAddressFormat,
        ConfigurationError,
        SignalNotSupported,
        CoreBug,
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
        : _errorCode(e)
        , _logMsg(argsToString(Timestamp {}, convertErrorToExplanation(e), std::forward<Args>(args)...)) {}

    static constexpr std::string_view convertErrorToExplanation(ErrorCode e) noexcept {
        switch (e) {
            case ErrorCode::Uninit: return "";
            case ErrorCode::InvalidPortFormat: return "Invalid port format, example input \"tcp://127.0.0.1:2345\"";
            case ErrorCode::InvalidAddressFormat:
                return "Invalid address format, example input \"tcp://127.0.0.1:2345\"";
            case ErrorCode::ConfigurationError:
                return "An error generated by system call that's likely due to mis-configuration";
            case ErrorCode::SignalNotSupported:
                return "A function call was interrupted by signal, but signal handling is not supported";
            case ErrorCode::CoreBug: return "Likely a bug within the library";
        }
        std::abort();
    }

    constexpr const char* what() const noexcept override { return _logMsg.c_str(); }

    const ErrorCode _errorCode;
    const std::string _logMsg;
};

}  // namespace ymq
}  // namespace scaler

template <>
struct std::formatter<scaler::ymq::Error, char> {
    template <class ParseContext>
    constexpr ParseContext::iterator parse(ParseContext& ctx) noexcept {
        return ctx.begin();
    }

    template <class FmtContext>
    constexpr FmtContext::iterator format(scaler::ymq::Error e, FmtContext& ctx) const noexcept {
        return std::ranges::copy(e._logMsg, ctx.out()).out;
    }
};

using UnrecoverableErrorFunctionHookPtr = std::function<void(scaler::ymq::Error)>;

[[noreturn]] constexpr inline void defaultUnrecoverableError(scaler::ymq::Error e) noexcept {
    std::print(stderr, "{}\n", e);
    exit(1);
}

inline UnrecoverableErrorFunctionHookPtr unrecoverableErrorFunctionHookPtr = defaultUnrecoverableError;

[[noreturn]] constexpr inline void unrecoverableError(scaler::ymq::Error e) {
    unrecoverableErrorFunctionHookPtr(std::move(e));
    exit(1);
}
