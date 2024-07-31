import socket


def get_available_tcp_port() -> int:
    with socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM) as sock:
        sock.bind(("0.0.0.0", 0))
        return sock.getsockname()[1]
