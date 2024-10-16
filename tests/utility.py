import logging
import socket
import unittest


def get_available_tcp_port(hostname: str = "127.0.0.1") -> int:
    with socket.socket(socket.AddressFamily.AF_INET, socket.SocketKind.SOCK_STREAM) as sock:
        sock.bind((hostname, 0))
        return sock.getsockname()[1]


def logging_test_name(obj: unittest.TestCase):
    logging.info(f"{obj.__class__.__name__}:{obj._testMethodName} ==============================================")
