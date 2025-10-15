"""
This MITM drops a % of packets
"""

import random
from typing import Optional

from tests.cpp.ymq.py_mitm.types import IP, AbstractMITM, TCPConnection, TunTapInterface


class MITM(AbstractMITM):
    def __init__(self, drop_pcent: str):
        self.drop_pcent = float(drop_pcent)

    def proxy(
        self,
        tuntap: TunTapInterface,
        pkt: IP,
        sender: TCPConnection,
        client_conn: Optional[TCPConnection],
        server_conn: TCPConnection,
    ) -> bool:
        if random.random() < self.drop_pcent:
            return False

        if sender == client_conn or client_conn is None:
            tuntap.send(server_conn.rewrite(pkt))
        elif sender == server_conn:
            tuntap.send(client_conn.rewrite(pkt))
        return True
