#pragma once

// C++
// #include <map>
// #include <optional>
#include <memory>
#include <optional>

// First-party
#include "scaler/io/ymq/configuration.h"
#include "scaler/io/ymq/event_loop_thread.h"
#include "scaler/io/ymq/tcp_client.h"
#include "scaler/io/ymq/tcp_server.h"
#include "scaler/io/ymq/typedefs.h"

using Identity = Configuration::Identity;

class TCPClient;
class TCPServer;

class EventLoopThread;

class IOSocket {
    std::shared_ptr<EventLoopThread> _eventLoopThread;
    Identity _identity;
    IOSocketType _socketType;

    std::optional<TcpClient> _tcpClient;
    std::optional<TcpServer> _tcpServer;
    // std::map<int /* class FileDescriptor */, MessageConnectionTCP*> fdToConnection;
    // std::map<std::string, MessageConnectionTCP*> identityToConnection;

public:
    IOSocket(std::shared_ptr<EventLoopThread> eventLoopThread, Identity identity, IOSocketType socketType)
        : _eventLoopThread(eventLoopThread), _identity(identity), _socketType(socketType) {}

    IOSocket(const IOSocket&)            = delete;
    IOSocket& operator=(const IOSocket&) = delete;
    IOSocket(IOSocket&&)                 = delete;
    IOSocket& operator=(IOSocket&&)      = delete;

    Identity identity() const { return _identity; }
    IOSocketType socketType() const { return _socketType; }

    // string -> connection mapping
    // and connection->string mapping

    // put it into the concurrent q, which is execute_now
    // void sendMessage(Message* msg, Continuation cont) {
    // EXAMPLE
    // execute_now(
    // switch (socketTypes) {
    //     case Pub:
    //         for (auto [fd, conn] &: fd_to_conn) {
    //             conn.send(msg.len, msg.size);
    //             conn.setWriteCompleteCallback(cont);
    //             eventLoopThread.getEventLoop().update_events(turn write on for this fd);
    //         }
    //         break;
    // }
    // )
    // }

    void onCreated();

    // void recvMessage(Message* msg);
};
