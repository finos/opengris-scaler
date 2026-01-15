#pragma once

#include <cassert>
#include <chrono>
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
namespace ymq {

// Simple timestamp utility
struct Timestamp {
    std::chrono::time_point<std::chrono::system_clock> timestamp;

    friend std::strong_ordering operator<=>(Timestamp x, Timestamp y) { return x.timestamp <=> y.timestamp; }

    Timestamp(): timestamp(std::chrono::system_clock::now()) {}
    Timestamp(std::chrono::time_point<std::chrono::system_clock> t) { timestamp = std::move(t); }

    template <class Rep, class Period = std::ratio<1>>
    Timestamp createTimestampByOffsetDuration(std::chrono::duration<Rep, Period> offset)
    {
        return {timestamp + offset};
    }
};

// For possibly logging purposes
inline std::string stringifyTimestamp(Timestamp ts)
{
    std::ostringstream oss;
    const auto ts_point {std::chrono::floor<std::chrono::seconds>(ts.timestamp)};
    const std::chrono::zoned_time z {std::chrono::current_zone(), ts_point};
    oss << std::format("{0:%F %T%z}", z);
    return oss.str();
}

inline std::ostream& operator<<(std::ostream& os, const Timestamp& ts)
{
    // Use the existing stringify function
    os << stringifyTimestamp(ts);
    return os;
}

#ifdef __linux__
// For timerfd
inline itimerspec convertToItimerspec(Timestamp ts)
{
    itimerspec timerspec {};
    const auto duration = ts.timestamp - std::chrono::system_clock::now();
    assert(duration.count() >= 0);

    const auto secs            = std::chrono::duration_cast<std::chrono::seconds>(duration);
    const auto nanosecs        = std::chrono::duration_cast<std::chrono::nanoseconds>(duration - secs);
    timerspec.it_value.tv_sec  = secs.count();
    timerspec.it_value.tv_nsec = nanosecs.count();

    return timerspec;
}
#endif  // __linux__
#ifdef _WIN32
// For timerfd
inline LARGE_INTEGER convertToLARGE_INTEGER(Timestamp ts)
{
    const auto duration = ts.timestamp - std::chrono::system_clock::now();
    assert(duration.count() >= 0);
    const auto nanosecs            = std::chrono::duration_cast<std::chrono::nanoseconds>(duration);
    long long relativeHundredNanos = 1LL * nanosecs.count() / 100 * -1;
    return *(LARGE_INTEGER*)&relativeHundredNanos;
}
#endif  // _WIN32

}  // namespace ymq
}  // namespace scaler

template <>
struct std::formatter<scaler::ymq::Timestamp, char> {
    template <class ParseContext>
    constexpr ParseContext::iterator parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template <class FmtContext>
    constexpr FmtContext::iterator format(scaler::ymq::Timestamp e, FmtContext& ctx) const
    {
        std::ostringstream out;
        const auto ts {std::chrono::floor<std::chrono::seconds>(e.timestamp)};
        const std::chrono::zoned_time z {std::chrono::current_zone(), ts};
        out << std::format("{0:%F %T%z}", z);
        return std::ranges::copy(std::move(out).str(), ctx.out()).out;
    }
};
