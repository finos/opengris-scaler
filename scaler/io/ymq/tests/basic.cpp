#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <cerrno>
#include <chrono>
#include <functional>
#include <string>
#include <thread>

#include "../examples/common.h"
#include "common.h"
#include "scaler/io/ymq/examples/common.h"
#include "scaler/io/ymq/io_context.h"

using namespace scaler::ymq;
using namespace std::chrono_literals;

void server_main()
{
    IOContext context(1);

    auto socket = syncCreateSocket(context, IOSocketType::Binder, "server");
    syncBindSocket(socket, "tcp://127.0.0.1:25711");
    auto result = syncRecvMessage(socket);

    assert(result.has_value());
    assert(result->payload.as_string() == "yi er san si wu liu");

    context.removeIOSocket(socket);
}

void client_main(int delay)
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

void test(int client_delay_secs)
{
    std::thread server([] { exception_handler(server_main); });
    std::thread client([=] { exception_handler([=] { client_main(client_delay_secs); }); });

    server.join();
    client.join();
}

int main()
{
    harness("works", 5, [] { test(3); });
    harness("broken", 5, [] { test(0); });
}
