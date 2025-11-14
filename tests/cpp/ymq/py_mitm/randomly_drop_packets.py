"""
This MITM drops a % of packets
"""

import random
from typing import Optional

from tests.cpp.ymq.py_mitm.mitm_types import IP, AbstractMITM, AbstractMITMInterface, TCPConnection


class MITM(AbstractMITM):
    def __init__(self, drop_percentage: str):
        self._drop_percentage = float(drop_percentage)
        self._consecutive_drop_limit = 3
        self._client_consecutive_drops = 0
        self._server_consecutive_drops = 0

    @property
    def can_drop_client(self) -> bool:
        return self._client_consecutive_drops < self._consecutive_drop_limit

    @property
    def can_drop_server(self) -> bool:
        return self._server_consecutive_drops < self._consecutive_drop_limit

    def proxy(
        self,
        tuntap: AbstractMITMInterface,
        pkt: IP,
        sender: TCPConnection,
        client_conn: Optional[TCPConnection],
        server_conn: TCPConnection,
    ) -> bool:
        drop = random.random() < self._drop_percentage

        if sender == client_conn or client_conn is None:
            if self.can_drop_client and drop:
                self._client_consecutive_drops += 1
                return False

            self._client_consecutive_drops = 0
            tuntap.send(server_conn.rewrite(pkt))
        elif sender == server_conn:
            if self.can_drop_server and drop:
                self._server_consecutive_drops += 1
                return False

            self._server_consecutive_drops == 0
            tuntap.send(client_conn.rewrite(pkt))
        return True
