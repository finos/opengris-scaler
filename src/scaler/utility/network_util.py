import functools
import socket


def get_available_tcp_port(hostname: str = "127.0.0.1") -> int:
    with socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM) as sock:
        sock.bind((hostname, 0))
        return sock.getsockname()[1]


def is_tcp_port_available(hostname: str, port: int) -> bool:
    with socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM) as sock:
        try:
            sock.bind((hostname, port))
        except OSError:
            return False
        return True


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
