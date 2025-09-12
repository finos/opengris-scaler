"""
This MITM inserts an unexpected TCP RST
"""

from core import MITMProtocol, TunTapInterface, IP, TCP, TCPConnection


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
    ) -> None:
        if sender == client_conn:
            if pkt[TCP].flags == "PA":
                self.client_pshack_counter += 1

                # on the second psh-ack, send a rst instead
                if self.client_pshack_counter == 2:
                    rst_pkt = IP(
                        src=client_conn.local_ip,
                        dst=client_conn.remote_ip
                    ) / TCP(
                        sport=client_conn.local_port,
                        dport=client_conn.remote_port, flags="R", seq=pkt[TCP].ack)
                    print(f"<- [{rst_pkt[TCP].flags}] (simulated)")
                    tuntap.send(rst_pkt)
                    return

            tuntap.send(server_conn.rewrite(pkt))
        elif sender == server_conn:
            tuntap.send(client_conn.rewrite(pkt))
