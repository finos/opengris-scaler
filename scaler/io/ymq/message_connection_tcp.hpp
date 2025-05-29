#pragma once

#include <optional>
#include <tuple>

#include "file_descriptor.hpp"
#include "message_connection.hpp"

class TcpWriteOperation {};
class TcpReadOperation {};

class MessageConnectionTCP: public MessageConnection {
    FileDescriptor fd;

    TcpWriteOperation write_op;
    TcpReadOperation read_op;

public:
    void send(Bytes data, SendMessageContinuation k) { todo(); }
    void recv(RecvMessageContinuation k) { todo(); }
};
