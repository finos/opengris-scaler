#pragma once

#include <cassert>
#include <chrono>
#include <format>
#include <iomanip>
#include <ostream>
#include <sstream>  // stringify

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
namespace utility {

// Simple timestamp utility
struct Timestamp {
    std::chrono::time_point<std::chrono::system_clock> timestamp;

    Timestamp(): timestamp(std::chrono::system_clock::now()) {}
    Timestamp(std::chrono::time_point<std::chrono::system_clock> t) { timestamp = std::move(t); }

    template <typename Rep, typename Period = std::ratio<1>>
    Timestamp operator+(std::chrono::duration<Rep, Period> offset) const
    {
        return {timestamp + offset};
    }

    template <typename Rep, typename Period = std::ratio<1>>
    Timestamp operator-(std::chrono::duration<Rep, Period> offset) const
    {
        return {timestamp - offset};
    }

    friend std::strong_ordering operator<=>(Timestamp x, Timestamp y) { return x.timestamp <=> y.timestamp; }

    friend std::chrono::milliseconds operator-(Timestamp lhs, Timestamp rhs)
    {
        return std::chrono::duration_cast<std::chrono::milliseconds>(lhs.timestamp - rhs.timestamp);
    }
};

inline std::string stringifyTimestamp(Timestamp ts)
{
    const auto ts_seconds {std::chrono::floor<std::chrono::seconds>(ts.timestamp)};
    const std::time_t system_time = std::chrono::system_clock::to_time_t(ts_seconds);
    const std::tm local_time      = *std::localtime(&system_time);

    std::ostringstream oss;
    oss << std::put_time(&local_time, "%F %T%z");
    return oss.str();
}

inline std::ostream& operator<<(std::ostream& os, const Timestamp& ts)
{
    os << stringifyTimestamp(ts);
    return os;
}

#ifdef __linux__
// For timerfd
inline itimerspec convertToItimerspec(Timestamp ts)
{
    using namespace std::chrono;

    itimerspec timerspec {};
    const auto duration = ts.timestamp - std::chrono::system_clock::now();
    assert(duration.count() >= 0);

    const auto secs            = duration_cast<seconds>(duration);
    const auto nanosecs        = duration_cast<nanoseconds>(duration - secs);
    timerspec.it_value.tv_sec  = secs.count();
    timerspec.it_value.tv_nsec = nanosecs.count();

    return timerspec;
}
#endif  // __linux__
#ifdef _WIN32
// For timerfd
inline LARGE_INTEGER convertToLARGE_INTEGER(Timestamp ts)
{
    using namespace std::chrono;
    const auto duration = ts.timestamp - std::chrono::system_clock::now();
    assert(duration.count() >= 0);
    const auto nanosecs            = duration_cast<nanoseconds>(duration);
    long long relativeHundredNanos = 1LL * nanosecs.count() / 100 * -1;
    return *(LARGE_INTEGER*)&relativeHundredNanos;
}
#endif  // _WIN32

}  // namespace utility
}  // namespace scaler

template <>
struct std::formatter<scaler::utility::Timestamp, char> {
    template <typename ParseContext>
    constexpr ParseContext::iterator parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template <typename FmtContext>
    constexpr FmtContext::iterator format(scaler::utility::Timestamp ts, FmtContext& ctx) const
    {
        return std::format_to(ctx.out(), "{}", scaler::utility::stringifyTimestamp(ts));
    }
};
