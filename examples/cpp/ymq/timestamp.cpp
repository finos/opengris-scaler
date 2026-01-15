#include "scaler/ymq/timestamp.h"

#include <iostream>

using scaler::ymq::stringifyTimestamp;
using scaler::ymq::Timestamp;

int main()
{
    Timestamp ts;
    std::cout << ts.timestamp << std::endl;
    Timestamp three_seconds_later_than_ts = ts.createTimestampByOffsetDuration(std::chrono::seconds(3));
    std::cout << three_seconds_later_than_ts.timestamp << std::endl;
    std::cout << stringifyTimestamp(ts) << std::endl;
    // a timestamp is smaller iff it is closer to the beginning of the world
    if (ts < three_seconds_later_than_ts) {
        std::cout << "ts happen before than three_seconds_later_than_ts.\n";
    }
}
