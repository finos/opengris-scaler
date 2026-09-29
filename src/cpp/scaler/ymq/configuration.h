#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>

namespace scaler {
namespace ymq {

// Expect all connections to start with this string.
constexpr std::array<uint8_t, 4> magicString {'Y', 'M', 'Q', 2};

constexpr size_t defaultClientMaxRetryTimes = 8;
constexpr std::chrono::milliseconds defaultClientInitRetryDelay {100};

constexpr int serverListenBacklog = 1024;

// Maximum buffer size for a single write() syscall.
//
// Some OSes discourage large writes (macOS, Windows).
constexpr size_t maxWriteBufferSize = 256ULL * 1024ULL * 1024ULL;  // 256 MB

// How long a TCP connection may sit idle before keep-alive probes start.
//
// Connections routinely go idle for the length of a task: a processor fetches its arguments, computes for
// minutes or hours, then writes its result. Without probes, a peer that died or a middlebox (NAT, firewall,
// load balancer) that dropped the flow goes unnoticed until that final write, which fails after the work it
// carries can no longer be reproduced. Well under the idle timeouts such devices commonly enforce.
constexpr unsigned int tcpKeepAliveDelaySeconds = 30;

// How long a BinderSocket remembers a disconnected peer's identity so that subsequent
// sendMessage() calls to it fail fast instead of queueing in _pendingSendMessages. The window
// only needs to bracket the worst-case lag between libuv processing the disconnect and the user
// (e.g. the Python asyncio layer) catching up on messages libuv buffered from that peer; that
// lag is millisecond-scale in practice, single-digit seconds even under heavy load.
constexpr std::chrono::seconds disconnectedIdentityTTL {60};

}  // namespace ymq
}  // namespace scaler
