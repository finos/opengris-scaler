"""
This MITM inserts an unexpected TCP RST
"""

from typing import Optional

from tests.cpp.ymq.py_mitm.mitm_types import IP, MITM, TCP, MITMInterface, TCPConnection


class SendRSTToClientMITM(MITM):
    def __init__(self):
        # count the number of psh-acks sent by the client
        self._client_pshack_counter = 0
        self._client_sent_identity = 0

    def proxy(
        self,
        tuntap: MITMInterface,
        pkt: IP,
        sender: TCPConnection,
        client_conn: Optional[TCPConnection],
        server_conn: TCPConnection,
    ) -> bool:
        if sender == client_conn or client_conn is None:
            if pkt[TCP].flags == "PA":
                self._client_pshack_counter += 1

                if bytes(pkt[TCP].payload).endswith(b"client") and self._client_sent_identity == 0:
                    self._client_sent_identity = 1

                # on the second psh-ack, send a rst instead
                # if self._client_pshack_counter == 2:
                elif self._client_sent_identity == 1:
                    self._client_sent_identity = 2
                    rst_pkt = IP(src=client_conn.local_ip, dst=client_conn.remote_ip) / TCP(
                        sport=client_conn.local_port, dport=client_conn.remote_port, flags="R", seq=pkt[TCP].ack
                    )
                    print(f"<- [{rst_pkt[TCP].flags}] (simulated)")
                    tuntap.send(rst_pkt)
                    return False

            tuntap.send(server_conn.rewrite(pkt))
        elif sender == server_conn:
            tuntap.send(client_conn.rewrite(pkt))
        return True
