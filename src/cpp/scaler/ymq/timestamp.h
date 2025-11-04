#pragma once

#include "scaler/ymq/internal/defs.h"

#include <cassert>
#include <chrono>
#include <ostream>
#include <sstream>  // stringify

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
