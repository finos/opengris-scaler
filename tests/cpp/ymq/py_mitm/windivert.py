import socket
import threading
from typing import Any

import pydivert
from scapy.all import IP, Packet  # type: ignore[attr-defined]

from tests.cpp.ymq.py_mitm.mitm_types import AbstractMITMInterface


class WindivertMITMInterface(AbstractMITMInterface):
    _windivert: pydivert.WinDivert
    _binder: socket.socket
    _accept_thread: threading.Thread
    _running: bool

    __interface: Any
    __direction: pydivert.Direction

    def __init__(self, local_ip: str, local_port: int, remote_ip: str, server_port: int):
        self._windivert = pydivert.WinDivert(f"tcp.DstPort == {local_port} or tcp.SrcPort == {server_port}")
        self._windivert.open()

        self._binder = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._binder.bind((local_ip, local_port))
        self._binder.listen(16)

        self._accept_thread = threading.Thread(target=self._accept_connections, daemon=True)
        self._accept_thread.start()

    def __del__(self) -> None:
        """Clean up resources and stop the accept thread."""
        try:
            self._binder.close()
        except Exception:
            pass

    def _accept_connections(self) -> None:
        """Accept all incoming connections in the background."""
        clients = []

        try:
            client, address = self._binder.accept()
            clients.append(client)
        except Exception:
            # Socket might be closed or other error occurred
            return

    def recv(self) -> Packet:
        windivert_packet = self._windivert.recv()

        # save these for later when we need to re-inject
        self.__interface = windivert_packet.interface
        self.__direction = windivert_packet.direction

        scapy_packet = IP(bytes(windivert_packet.raw))
        return scapy_packet

    def send(self, pkt: Packet) -> None:
        self._windivert.send(pydivert.Packet(bytes(pkt), self.__interface, self.__direction))
