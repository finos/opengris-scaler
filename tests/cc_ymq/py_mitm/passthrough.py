"""
This MITM acts as a transparent passthrough, it simply forwards packets as they are,
minus necessary header changes to retransmit
This MITM should have no effect on the client and server,
and they should behave as if the MITM is not present
"""

from tests.cc_ymq.py_mitm.types import MITMProtocol, TunTapInterface, IP, TCPConnection
from typing import Optional


class MITM(MITMProtocol):
    def proxy(
        self,
        tuntap: TunTapInterface,
        pkt: IP,
        sender: TCPConnection,
        client_conn: Optional[TCPConnection],
        server_conn: TCPConnection,
    ) -> bool:
        if sender == client_conn or client_conn is None:
            tuntap.send(server_conn.rewrite(pkt))
        elif sender == server_conn:
            tuntap.send(client_conn.rewrite(pkt))
        return True
