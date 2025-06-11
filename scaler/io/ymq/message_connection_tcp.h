#pragma once

#include <optional>
#include <tuple>

#include "scaler/io/ymq/file_descriptor.h"
#include "scaler/io/ymq/message_connection.h"

class TcpWriteOperation {};
class TcpReadOperation {};

class MessageConnectionTCP: public MessageConnection {
    FileDescriptor fd;

    TcpWriteOperation write_op;
    TcpReadOperation read_op;

public:
    void send(Bytes data, SendMessageContinuation k);
    void recv(RecvMessageContinuation k);
};
