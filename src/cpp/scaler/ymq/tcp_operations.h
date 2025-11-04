#pragma once

#include <cstdint>  // uint64_t

#include "scaler/ymq/bytes.h"
#include "scaler/ymq/configuration.h"
#include "scaler/ymq/message.h"

namespace scaler {
namespace ymq {

struct TcpReadOperation {
    size_t _cursor {};
    uint64_t _header {};
    Bytes _payload {};
};

struct TcpWriteOperation {
    using SendMessageCallback = Configuration::SendMessageCallback;
    uint64_t _header;
    Bytes _address;
    Bytes _payload;
    SendMessageCallback _callbackAfterCompleteWrite;

    TcpWriteOperation(Message msg, SendMessageCallback callbackAfterCompleteWrite) noexcept
        : _header(msg.payload.len())
        , _payload(std::move(msg.payload))
        , _callbackAfterCompleteWrite(std::move(callbackAfterCompleteWrite))
    {
    }

    TcpWriteOperation(Bytes payload, SendMessageCallback callbackAfterCompleteWrite) noexcept
        : _header(payload.len())
        , _payload(std::move(payload))
        , _callbackAfterCompleteWrite(std::move(callbackAfterCompleteWrite))
    {
    }
};

}  // namespace ymq
}  // namespace scaler
