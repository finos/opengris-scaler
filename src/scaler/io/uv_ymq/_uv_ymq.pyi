# NOTE: NOT IMPLEMENTATION, TYPE INFORMATION ONLY
# This file contains type stubs for the UV YMQ Python C Extension module

from enum import IntEnum

class AddressType(IntEnum):
    """Address type enum"""

    IPC = 0
    TCP = 1

class Address:
    """
    A socket address, can either be a TCP address (IPv4/6) or an IPC path.

    Example address strings:
        - ipc://some_ipc_socket_name
        - tcp://127.0.0.1:1827
        - tcp://[2001:db8::1]:1211
    """

    type: AddressType
    """Get the address type (IPC or TCP)"""

    def __init__(self, address: str) -> None:
        """
        Create an Address from a string.

        Args:
            address: Address string (e.g., "tcp://127.0.0.1:9000", "ipc://my_socket")

        Raises:
            ValueError: If the address format is invalid
        """

    def __repr__(self) -> str: ...

class IOContext:
    """Manages a pool of IO event threads"""

    num_threads: int
    """Get the number of threads in the IOContext"""

    def __init__(self, num_threads: int = 1) -> None:
        """Create an IOContext with the specified number of threads"""

    def __repr__(self) -> str: ...
