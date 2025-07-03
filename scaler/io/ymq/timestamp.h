#pragma once

#include <sys/time.h>  // itimerspec

#include <chrono>
#include <sstream>  // stringify

// Simple timestamp utility
struct Timestamp {
    std::chrono::time_point<std::chrono::system_clock> timestamp;

    friend std::strong_ordering operator<=>(Timestamp x, Timestamp y) { return x.timestamp <=> y.timestamp; }

    Timestamp(): timestamp(std::chrono::system_clock::now()) {}
    Timestamp(std::chrono::time_point<std::chrono::system_clock> t) { timestamp = std::move(t); }

    template <class Rep, class Period = std::ratio<1>>
    Timestamp createTimestampByOffsetDuration(std::chrono::duration<Rep, Period> offset) {
        return {timestamp + offset};
    }
};

// For possibly logging purposes
inline std::string stringifyTimestamp(Timestamp ts) {
    std::ostringstream oss;
    oss << ts.timestamp;
    return oss.str();
}

// For timerfd
inline itimerspec convertToItimerspec(Timestamp ts) {
    using namespace std::chrono;

    itimerspec timerspec {};
    const auto duration = ts.timestamp - std::chrono::system_clock::now();
    if (duration.count() < 0) {
        printf("NO THIS SHOULD NEVER HAPPEND\n");
        exit(-1);
        return timerspec;
    }

    const auto secs            = duration_cast<seconds>(duration);
    const auto nanosecs        = duration_cast<nanoseconds>(duration - secs);
    timerspec.it_value.tv_sec  = secs.count();
    timerspec.it_value.tv_nsec = nanosecs.count();

    return timerspec;
}
