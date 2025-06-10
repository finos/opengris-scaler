#include "scaler/io/ymq/timestamp.h"

#include <iostream>

using namespace std::chrono_literals;

int main() {
    Timestamp ts;
    std::cout << ts.timestamp << std::endl;
    Timestamp three_seconds_later_than_ts = ts.createTimestampByOffsetDuration(3s);
    std::cout << three_seconds_later_than_ts.timestamp << std::endl;
    printf("%s\n", stringifyTimestamp(ts).c_str());
    // a timestamp is smaller iff it is closer to the beginning of the world
    if (ts < three_seconds_later_than_ts) {
        printf("ts happen before than three_seconds_later_than_ts\n");
    }
}
