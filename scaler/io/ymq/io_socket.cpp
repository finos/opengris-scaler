#include "scaler/io/ymq/io_socket.h"

// NOTE: We need it after we put impl
#include "scaler/io/ymq/event_loop_thread.h"

void IOSocket::onCreated() {
    // Detect if we need to initialize tcpClient and/or tcpServer
    // If so, initialize it, and then call their onAdd();
    if (_socketType == IOSocketType::Router) {
        // assert(!tcpClient);
        _tcpClient.emplace(_eventLoopThread);
        // assert(!tcpServer);
        _tcpServer.emplace(_eventLoopThread);
        _tcpClient->onCreated();
        _tcpServer->onCreated();
    }
    // Different SocketType might have different rules
}
