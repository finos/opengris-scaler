import functools
import socket

# binding port 0 makes the OS pick a free port at bind time, with no window for another process to take it
OS_PICKED_PORT = 0

# hostnames that name no machine: container runtimes such as OCI container instances leave the hostname unset
UNSET_HOSTNAMES = frozenset({"", "localhost", "localhost.localdomain"})

# TEST-NET-1 (RFC 5737): connecting a UDP socket to it sends nothing, it only picks the default route's interface
ROUTE_PROBE_ADDRESS = ("192.0.2.1", 9)


@functools.lru_cache(maxsize=1)
def get_hostname() -> str:
    """The machine this process runs on, or its IP when the hostname is unset. Cached: the heartbeats are hot."""
    try:
        hostname = socket.gethostname()
    except OSError:
        hostname = ""

    if hostname not in UNSET_HOSTNAMES:
        return hostname

    return _get_default_route_ip() or hostname


def _get_default_route_ip() -> str:
    try:
        with socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_DGRAM) as sock:
            sock.connect(ROUTE_PROBE_ADDRESS)
            return sock.getsockname()[0]
    except OSError:
        return ""
