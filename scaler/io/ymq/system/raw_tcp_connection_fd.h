#pragma once

#include <cstdint>  // uint64_t
#include <expected>
#include <vector>

namespace scaler {
namespace ymq {

struct RawTCPConnectionFD {
    enum IOError {
        Drained,
        Aborted,
        Disconnected,
    };

    std::expected<uint64_t, IOError> readBytes(char* dest, size_t len);
    std::expected<uint64_t, IOError> writeBytes(std::vector<std::pair<const unsigned char*, size_t>> arg);

    // I need the semantic that, if this return certain value,
    // i need to immediatley read bytes
    bool prepareReadBytes(void* notifyHandle);
    // TODO: This might need error handling
    std::pair<size_t, bool> prepareWriteBytes(char* dest, size_t len, void* notifyHandle);

    // TODO: Semantics of destruction
    uint64_t _fd;

    // TODO: Maybe take a reference
    RawTCPConnectionFD(uint64_t fd): _fd(fd) {}
};

// class RawTCPAcceptFD {
//     uint64_t _fd;
//     // Name TBD
//     auto prepareAcceptConnection();
//     auto acceptConnection();
// };
//
// class RawTCPConnectFD {
//     uint64_t _fd;
//     // Name TBD
//     auto prepareConnectConnection();
//     auto connectConnection();
// };
//
// class MessageStream {
//     using ReturnType = void;
//     ReturnType tryReadOneMessage();
//     ReturnType tryReadAllMessages();
//     ReturnType tryWriteOneMessage();
//     ReturnType tryWriteAllMessages();
//
//     enum class IOError {
//         Drained,
//         Aborted,
//         Disconnected,
//         MessageTooLarge,
//     };
// };
}  // namespace ymq
}  // namespace scaler
