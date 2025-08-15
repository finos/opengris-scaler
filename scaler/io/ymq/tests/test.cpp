#include <cassert>
#include <cstdlib>
#include <print>
#include <string>
#include <thread>

#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/tests/basic.h"
#include "scaler/io/ymq/tests/common.h"
#include "scaler/io/ymq/tests/incomplete_identity.h"
#include "scaler/io/ymq/tests/reconnect.h"
#include "scaler/io/ymq/tests/slow.h"
#include "scaler/io/ymq/tests/bad_header.h"
#include "scaler/io/ymq/tests/empty_message.h"

using namespace scaler::ymq;
using namespace std::chrono_literals;

int main()
{
    setup_signal_handlers();

    Counters counters;

    counters += test("basic, works", 10, [] { basic_client_main(3); }, basic_server_main);
    counters += test("basic, broken", 10, [] { basic_client_main(0); }, basic_server_main);
    counters += test("slow network", 20, slow_client_main, slow_server_main);
    counters += test("incomplete identity", 20, incomplete_identity_client_main, incomplete_identity_server_main);

    // this is wip
    // ReconnectContext rectx;
    // counters +=
    //     test("reconnect", 15, [rectx] { reconnect_client_main(rectx); }, [rectx] { reconnect_server_main(rectx); });

    // this throws `std::bad_alloc` which shuts down the program -- is it possible to recover from this?
    // counters += test("bad header", 10, bad_header_client_main, bad_header_server_main);

    counters += test("empty message", 10, empty_message_client_main, empty_message_server_main);

    std::println("{}/{} tests passed", counters.successes, counters.total());

    if (counters.failures > 0)
        std::exit(EXIT_FAILURE);
    std::exit(EXIT_SUCCESS);
}
