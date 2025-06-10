#include "scaler/io/ymq/timed_queue.h"

int main() {
    TimedQueue tq;
    Timestamp ts;

    tq.push(ts, [] { printf("in timer\n"); });
}
