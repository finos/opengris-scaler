"""
This MITM drops a % of packets
"""

import random
from core import MITMProtocol, TunTapInterface, IP, TCPConnection


class MITM(MITMProtocol):
    def __init__(self, drop_pcent: str):
        self.counter = 0
        self.drop_pcent = float(drop_pcent)

    def proxy(
        self,
        tuntap: TunTapInterface,
        pkt: IP,
        sender: TCPConnection,
        client_conn: TCPConnection | None,
        server_conn: TCPConnection,
    ) -> bool:
        # the counter stops us from dropping consecutively
        if self.counter > 1 and random.random() < self.drop_pcent:
            print("[!] Dropping packet")
            self.counter = 0
            return False

        if sender == client_conn or client_conn is None:
            tuntap.send(server_conn.rewrite(pkt))
        elif sender == server_conn:
            tuntap.send(client_conn.rewrite(pkt))

        self.counter += 1
        return True
