"""
This MITM inserts an unexpected TCP RST
"""

from tests.cc_ymq.py_mitm.types import IP, TCP, MITMProtocol, TCPConnection, TunTapInterface


class MITM(MITMProtocol):
    def __init__(self):
        # count the number of psh-acks sent by the client
        self.client_pshack_counter = 0

    def proxy(
        self,
        tuntap: TunTapInterface,
        pkt: IP,
        sender: TCPConnection,
        client_conn: TCPConnection | None,
        server_conn: TCPConnection,
    ) -> bool:
        if sender == client_conn or client_conn is None:
            if pkt[TCP].flags == "PA":
                self.client_pshack_counter += 1

                # on the second psh-ack, send a rst instead
                if self.client_pshack_counter == 2:
                    rst_pkt = IP(src=client_conn.local_ip, dst=client_conn.remote_ip) / TCP(
                        sport=client_conn.local_port, dport=client_conn.remote_port, flags="R", seq=pkt[TCP].ack
                    )
                    print(f"<- [{rst_pkt[TCP].flags}] (simulated)")
                    tuntap.send(rst_pkt)
                    return True

            tuntap.send(server_conn.rewrite(pkt))
        elif sender == server_conn:
            tuntap.send(client_conn.rewrite(pkt))
        return True


# client -> mitm -> server
# server -> mitm -> client

# client: 127.0.0.1:8080
# mitm: 127.0.0.1:8081
# server: 127.0.0.1:8081


# client -> mitm == src = client.ip, sport = client.port ;; dst = mitm.ip, dport = mitm.port
# mitm -> server == src = mitm.ip, sport = mitm.port ;; dst = server.ip, dport = server.port
