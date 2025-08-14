#include <cassert>
#include <chrono>
#include <cstdlib>
#include <print>
#include <string>
#include <thread>

#include "../examples/common.h"
#include "common.h"
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"

using namespace scaler::ymq;
using namespace std::chrono_literals;

void basic_server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25711");
    auto result = syncRecvMessage(socket);

    assert(result.has_value());
    assert(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);
}

void basic_client_main(int delay)
{
    TcpSocket socket;

    socket.connect("127.0.0.1", 25711);

    socket.write_message("client");
    auto remote_identity = socket.read_message();
    assert(remote_identity == "server");
    socket.write_message("yi er san si wu liu");

    if (delay)
        std::this_thread::sleep_for(std::chrono::seconds(delay));
}

int main()
{
    setup_signal_handlers();

    Counters counters;

    counters += harness("basic, works", 10, [] { basic_client_main(3); }, basic_server_main);
    counters += harness("basic, broken", 10, [] { basic_client_main(0); }, basic_server_main);
    counters += harness(
        "slow network",
        20,
        [] {
            TcpSocket socket;

            socket.connect("127.0.0.1", 25713);

            socket.write_message("client");
            auto remote_identity = socket.read_message();
            assert(remote_identity == "server");

            std::string message = "yi er san si wu liu";
            uint64_t header     = message.length();

            socket.write_all((char*)&header, 4);
            std::this_thread::sleep_for(5s);
            socket.write_all((char*)&header + 4, 4);
            std::this_thread::sleep_for(3s);
            socket.write_all(message.data(), header / 2);
            std::this_thread::sleep_for(5s);
            socket.write_all(message.data() + header / 2, header - header / 2);
            std::this_thread::sleep_for(3s);
        },
        [] {
            IOContext context(1);

            auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
            syncBindSocket(socket, "tcp://127.0.0.1:25713");
            auto result = syncRecvMessage(socket);

            assert(result.has_value());
            assert(result->payload.as_string() == "yi er san si wu liu");

            context.removeIOSocket(socket);
        });

    std::println("{}/{} tests passed", counters.successes, counters.total());

    if (counters.failures > 0)
        std::exit(EXIT_FAILURE);
    std::exit(EXIT_SUCCESS);
}
