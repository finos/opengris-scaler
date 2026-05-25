"""
This MITM injects an RST to the client immediately after forwarding the server's YMQ
identity packet (the second PA from the server, following the magic).

After injecting the RST it creates a sentinel file at the path supplied as a constructor
argument. The test thread polls for that file before calling sendMessage(), so by the time
the write is issued the RST is already in the client socket's kernel receive buffer.
"""

from pathlib import Path
from typing import Optional

from tests.cpp.ymq.py_mitm.mitm_types import IP, MITM, TCP, MITMInterface, TCPConnection

_YMQ_MAGIC = b"YMQ\x01"


class SendRSTAfterServerIdentityMITM(MITM):
    def __init__(self, rendezvous_path: str):
        self._saw_server_magic = False
        self._rst_injected = False
        self._rendezvous_path = Path(rendezvous_path)

    def proxy(
        self,
        tuntap: MITMInterface,
        pkt: IP,
        sender: TCPConnection,
        client_conn: Optional[TCPConnection],
        server_conn: TCPConnection,
    ) -> bool:
        if sender == client_conn or client_conn is None:
            tuntap.send(server_conn.rewrite(pkt))
        elif sender == server_conn:
            tuntap.send(client_conn.rewrite(pkt))
            if pkt[TCP].flags == "PA" and not self._rst_injected:
                payload = bytes(pkt[TCP].payload)
                if payload.startswith(_YMQ_MAGIC):
                    self._saw_server_magic = True
                elif self._saw_server_magic:
                    self._rst_injected = True
                    next_seq = pkt[TCP].seq + len(payload)
                    rst_pkt = IP(src=client_conn.local_ip, dst=client_conn.remote_ip) / TCP(
                        sport=client_conn.local_port, dport=client_conn.remote_port, flags="R", seq=next_seq
                    )
                    print("<- [R] (simulated, after server identity)")
                    tuntap.send(rst_pkt)
                    self._rendezvous_path.touch()
        return True
