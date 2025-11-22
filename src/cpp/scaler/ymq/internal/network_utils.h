#pragma once
#include <algorithm>  // std::upper_bound

#include "scaler/ymq/internal/defs.h"
#include "scaler/ymq/internal/raw_connection_tcp_fd.h"

namespace scaler {
namespace ymq {

bool setReuseAddress(auto fd)
{
    int optval = 1;
    return !(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (const char*)&optval, sizeof(optval)) == -1);
}

std::pair<uint64_t, RawConnectionTCPFD::IOStatus> tryReadUntilComplete(void* dest, size_t size, auto readBytes)
{
    uint64_t cnt = 0;
    while (size) {
        const auto current = readBytes((char*)dest + cnt, size);
        if (current) {
            cnt += current.value();
            size -= current.value();
        } else {
            return {cnt, current.error()};
        }
    }
    return {cnt, RawConnectionTCPFD::IOStatus::MoreBytesAvailable};
}

std::pair<uint64_t, RawConnectionTCPFD::IOStatus> tryWriteUntilComplete(
    const std::vector<std::pair<void*, size_t>>& buffers, auto writeBytes)
{
    if (buffers.empty()) {
        return {0, RawConnectionTCPFD::IOStatus::MoreBytesAvailable};
    }

    std::vector<size_t> prefixSum(buffers.size() + 1);
    for (size_t i = 0; i < buffers.size(); ++i) {
        prefixSum[i + 1] = prefixSum[i] + buffers[i].second;
    }
    const size_t total = prefixSum.back();

    size_t sent = 0;
    while (sent != total) {
        auto unfinished = std::upper_bound(prefixSum.begin(), prefixSum.end(), sent);
        --unfinished;

        std::vector<std::pair<void*, size_t>> currentBuffers;

        auto begin          = buffers.begin() + std::distance(prefixSum.begin(), unfinished);
        const size_t remain = sent - *unfinished;

        currentBuffers.push_back({(char*)begin->first + remain, begin->second - remain});
        while (++begin != buffers.end()) {
            currentBuffers.push_back(*begin);
        }

        const auto res = writeBytes(currentBuffers);
        if (res) {
            sent += res.value();
        } else {
            return {sent, res.error()};
        }
    }
    return {total, RawConnectionTCPFD::IOStatus::MoreBytesAvailable};
}

}  // namespace ymq
}  // namespace scaler
