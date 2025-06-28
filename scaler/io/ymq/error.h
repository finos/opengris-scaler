#pragma once

#include <algorithm>
#include <cassert>
#include <cstring>
#include <exception>  // std::terminate
#include <format>
#include <print>
#include <sstream>
#include <string>
#include <vector>

struct Error {
    enum LogicError {
        InvalidAddressFormat  = -1,
        InvalidAddressFormat2 = -2,
    };

    constexpr const static std::array<const char*, 20> errorToExplanation {
        "Invalid address format, example input: \"tcp://127.0.0.1:2345\"",
    };

    constexpr static const char* convertErrorToExplanation(Error e) {
        if (e.error < 0) {
            return errorToExplanation[(-e.error - 1)];
        } else {
            return strerror(e.error);
        }
    }

    int error;
};

template <>
struct std::formatter<Error, char> {
    template <class ParseContext>
    constexpr ParseContext::iterator parse(ParseContext& ctx) {
        return ctx.begin();
    }

    template <class FmtContext>
    FmtContext::iterator format(Error e, FmtContext& ctx) const {
        std::ostringstream out;
        out << Error::convertErrorToExplanation(e);
        return std::ranges::copy(std::move(out).str(), ctx.out()).out;
    }
};

using UNRECOVERABLE_ERROR_FUNCTION_HOOK_PTR = void (*)(Error, std::string);

inline UNRECOVERABLE_ERROR_FUNCTION_HOOK_PTR unrecoverable_error_function_hook_ptr = nullptr;

template <std::size_t N>
    requires(N > 0)
consteval auto getFormatString() {
    std::string str = "{}";
    for (size_t i = 1; i < N; ++i)
        str += " {}";
    std::array<char, N * 3 - 1> arr;
    std::ranges::copy(str, arr.begin());
    return arr;
}

template <typename... Args>
constexpr void unrecoverableError(Error exc, Args&&... args) {
    static constexpr auto str = getFormatString<sizeof...(Args)>();
    if (!unrecoverable_error_function_hook_ptr) {
        std::print("{}. ", exc);
        std::println(std::string_view {str}, std::forward<Args>(args)...);
        std::terminate();
    } else {
        std::string res = std::format(std::string_view {str}, std::forward<Args>(args)...);
        unrecoverable_error_function_hook_ptr(exc, std::move(res));
    }
}
