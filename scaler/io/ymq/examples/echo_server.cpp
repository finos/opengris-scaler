// C
#include <stdio.h>

// First-party
#include "scaler/io/ymq/io_context.h"
#include "scaler/io/ymq/io_socket.h"
#include "scaler/io/ymq/typedefs.h"

// Goal:
// Make sure we can write an echo server with ymq in C++, pretend there is a language barrier, to mimic
// the behavior as if we are running with Python
// We should of course provide an echo client.

int main() {
    printf("Hello, world!\n");

    IOContext context;
    std::shared_ptr<IOSocket> socket = context.createIOSocket("ServerSocket", IOSocketType::Dealer);
    // char buf[8];
    // while (true) {
    //     socket.read("any_identity", buf, []() { printf("read completed\n"); });
    //     socket.write("reading_from_identity", buf, []() { printf("write completed\n"); });
    // }
    // printf("done");
}
