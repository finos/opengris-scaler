"""
This MITM drops a % of packets
"""

import random
from tests.cc_ymq.py_mitm.types import MITMProtocol, TunTapInterface, IP, TCPConnection


class MITM(MITMProtocol):
    def __init__(self, drop_pcent: str):
        self.drop_pcent = float(drop_pcent)

    def proxy(
        self,
        tuntap: TunTapInterface,
        pkt: IP,
        sender: TCPConnection,
        client_conn: TCPConnection | None,
        server_conn: TCPConnection,
    ) -> bool:
        if random.random() < self.drop_pcent:
            print("[!] Dropping packet")
            return False

        if sender == client_conn:
            tuntap.send(server_conn.rewrite(pkt))
        elif sender == server_conn:
            tuntap.send(client_conn.rewrite(pkt))
        return True
